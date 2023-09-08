use crate::{
	export::logs::{ExportResult, LogData, LogExporter},
	runtime::{RuntimeChannel, TrySend},
};
use futures_channel::oneshot;
use futures_util::{
	future::{self, BoxFuture, Either},
	select,
	stream::{FusedStream, FuturesUnordered},
	Stream, {stream, StreamExt as _},
};
#[cfg(feature = "logs_level_enabled")]
use opentelemetry_api::logs::Severity;
use opentelemetry_api::{
	global,
	logs::{LogError, LogResult},
};
use std::{env, str::FromStr, thread};
use std::{
	fmt::{self, Debug, Formatter},
	time::Duration,
};

/// The interface for plugging into a [`Logger`].
///
/// [`Logger`]: crate::logs::Logger
pub trait LogProcessor: Send + Sync + Debug {
	/// Called when a log record is ready to processed and exported.
	fn emit(&self, data: LogData);
	/// Force the logs lying in the cache to be exported.
	fn force_flush(&self) -> LogResult<()>;
	/// Shuts down the processor.
	fn shutdown(&mut self) -> LogResult<()>;
	#[cfg(feature = "logs_level_enabled")]
	/// Check if logging is enabled
	fn event_enabled(&self, level: Severity, target: &str, name: &str) -> bool;
}

/// A [`LogProcessor`] that exports synchronously when logs are emitted.
///
/// # Examples
///
/// Note that the simple processor exports synchronously every time a log is
/// emitted. If you find this limiting, consider the batch processor instead.
#[derive(Debug)]
pub struct SimpleLogProcessor {
	sender: crossbeam_channel::Sender<Option<LogData>>,
	shutdown: crossbeam_channel::Receiver<()>,
}

impl SimpleLogProcessor {
	pub(crate) fn new(mut exporter: Box<dyn LogExporter>) -> Self {
		let (log_tx, log_rx) = crossbeam_channel::unbounded();
		let (shutdown_tx, shutdown_rx) = crossbeam_channel::bounded(0);

		let _ = thread::Builder::new()
			.name("opentelemetry-log-exporter".to_string())
			.spawn(move || {
				while let Ok(Some(log)) = log_rx.recv() {
					if let Err(err) = futures_executor::block_on(exporter.export(vec![log])) {
						global::handle_error(err);
					}
				}

				exporter.shutdown();

				if let Err(err) = shutdown_tx.send(()) {
					global::handle_error(LogError::from(format!(
						"could not send shutdown: {:?}",
						err
					)));
				}
			});

		SimpleLogProcessor {
			sender: log_tx,
			shutdown: shutdown_rx,
		}
	}
}

struct BatchLogProcessorInternal<R> {
	logs: Vec<LogData>,
	export_tasks: FuturesUnordered<BoxFuture<'static, ExportResult>>,
	runtime: R,
	exporter: Box<dyn LogExporter>,
	config: BatchConfig,
}

impl<R: RuntimeChannel<BatchMessage>> BatchLogProcessorInternal<R> {
	async fn flush(&mut self, res_channel: Option<oneshot::Sender<ExportResult>>) {
		let export_task = self.export();
		let task = Box::pin(async move {
			let result = export_task.await;

			if let Some(channel) = res_channel {
				if let Err(result) = channel.send(result) {
					global::handle_error(LogError::from(format!(
						"failed to send flush result: {:?}",
						result
					)));
				}
			} else if let Err(err) = result {
				global::handle_error(err);
			}

			Ok(())
		});

		if self.config.max_concurrent_exports == 1 {
			let _ = task.await;
		} else {
			self.export_tasks.push(task);
			while self.export_tasks.next().await.is_some() {}
		}
	}

	/// Process a single message
	///
	/// A return value of false indicates shutdown
	async fn process_message(&mut self, message: BatchMessage) -> bool {
		match message {
			// Span has finished, add to buffer of pending spans.
			BatchMessage::ExportLog(log) => {
				self.logs.push(log);

				if self.logs.len() == self.config.max_export_batch_size {
					// If concurrent exports are saturated, wait for one to complete.
					if !self.export_tasks.is_empty()
						&& self.export_tasks.len() == self.config.max_concurrent_exports
					{
						self.export_tasks.next().await;
					}

					let export_task = self.export();
					let task = async move {
						if let Err(err) = export_task.await {
							global::handle_error(err);
						}

						Ok(())
					};
					// Special case when not using concurrent exports
					if self.config.max_concurrent_exports == 1 {
						let _ = task.await;
					} else {
						self.export_tasks.push(Box::pin(task));
					}
				}
			}
			// Span batch interval time reached or a force flush has been invoked, export
			// current spans.
			//
			// This is a hint to ensure that any tasks associated with Spans for which the
			// SpanProcessor had already received events prior to the call to ForceFlush
			// SHOULD be completed as soon as possible, preferably before returning from
			// this method.
			//
			// In particular, if any SpanProcessor has any associated exporter, it SHOULD
			// try to call the exporter's Export with all spans for which this was not
			// already done and then invoke ForceFlush on it. The built-in SpanProcessors
			// MUST do so. If a timeout is specified (see below), the SpanProcessor MUST
			// prioritize honoring the timeout over finishing all calls. It MAY skip or
			// abort some or all Export or ForceFlush calls it has made to achieve this
			// goal.
			//
			// NB: `force_flush` is not currently implemented on exporters; the equivalent
			// would be waiting for exporter tasks to complete. In the case of
			// channel-coupled exporters, they will need a `force_flush` implementation to
			// properly block.
			BatchMessage::Flush(res_channel) => {
				self.flush(res_channel).await;
			}
			// Stream has terminated or processor is shutdown, return to finish execution.
			BatchMessage::Shutdown(ch) => {
				self.flush(Some(ch)).await;
				self.exporter.shutdown();
				return false;
			}
		}

		true
	}

	fn export(&mut self) -> BoxFuture<'static, ExportResult> {
		// Batch size check for flush / shutdown. Those methods may be called
		// when there's no work to do.
		if self.logs.is_empty() {
			return Box::pin(future::ready(Ok(())));
		}

		let export = self.exporter.export(self.logs.split_off(0));
		let timeout = self.runtime.delay(self.config.max_export_timeout);
		let time_out = self.config.max_export_timeout;

		Box::pin(async move {
			match future::select(export, timeout).await {
				Either::Left((export_res, _)) => export_res,
				Either::Right((_, _)) => ExportResult::Err(LogError::ExportTimedOut(time_out)),
			}
		})
	}

	async fn run(mut self, mut messages: impl Stream<Item = BatchMessage> + Unpin + FusedStream) {
		loop {
			select! {
				// FuturesUnordered implements Fuse intelligently such that it
				// will become eligible again once new tasks are added to it.
				_ = self.export_tasks.next() => {
					// An export task completed; do we need to do anything with it?
				},
				message = messages.next() => {
					match message {
						Some(message) => {
							if !self.process_message(message).await {
								break;
							}
						},
						None => break,
					}
				},
			}
		}
	}
}

impl LogProcessor for SimpleLogProcessor {
	fn emit(&self, data: LogData) {
		if let Err(err) = self.sender.send(Some(data)) {
			global::handle_error(LogError::from(format!("error processing log {:?}", err)));
		}
	}

	fn force_flush(&self) -> LogResult<()> {
		// Ignored since all logs in Simple Processor will be exported as they ended.
		Ok(())
	}

	fn shutdown(&mut self) -> LogResult<()> {
		if self.sender.send(None).is_ok() {
			if let Err(err) = self.shutdown.recv() {
				global::handle_error(LogError::from(format!(
					"error shutting down log processor: {:?}",
					err
				)))
			}
		}
		Ok(())
	}

	#[cfg(feature = "logs_level_enabled")]
	fn event_enabled(&self, _level: Severity, _target: &str, _name: &str) -> bool {
		true
	}
}

/// A [`LogProcessor`] that asynchronously buffers log records and reports
/// them at a preconfigured interval.
pub struct BatchLogProcessor<R: RuntimeChannel<BatchMessage>> {
	message_sender: R::Sender,
}

impl<R: RuntimeChannel<BatchMessage>> Debug for BatchLogProcessor<R> {
	fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
		f.debug_struct("BatchLogProcessor")
			.field("message_sender", &self.message_sender)
			.finish()
	}
}

impl<R: RuntimeChannel<BatchMessage>> LogProcessor for BatchLogProcessor<R> {
	fn emit(&self, data: LogData) {
		let result = self.message_sender.try_send(BatchMessage::ExportLog(data));

		if let Err(err) = result {
			global::handle_error(LogError::Other(err.into()));
		}
	}

	#[cfg(feature = "logs_level_enabled")]
	fn event_enabled(&self, _level: Severity, _target: &str, _name: &str) -> bool {
		true
	}

	fn force_flush(&self) -> LogResult<()> {
		let (res_sender, res_receiver) = oneshot::channel();
		self.message_sender
			.try_send(BatchMessage::Flush(Some(res_sender)))
			.map_err(|err| LogError::Other(err.into()))?;

		futures_executor::block_on(res_receiver)
			.map_err(|err| LogError::Other(err.into()))
			.and_then(std::convert::identity)
	}

	fn shutdown(&mut self) -> LogResult<()> {
		let (res_sender, res_receiver) = oneshot::channel();
		self.message_sender
			.try_send(BatchMessage::Shutdown(res_sender))
			.map_err(|err| LogError::Other(err.into()))?;

		futures_executor::block_on(res_receiver)
			.map_err(|err| LogError::Other(err.into()))
			.and_then(std::convert::identity)
	}
}

impl<R: RuntimeChannel<BatchMessage>> BatchLogProcessor<R> {
	pub(crate) fn new(exporter: Box<dyn LogExporter>, config: BatchConfig, runtime: R) -> Self {
		let (message_sender, message_receiver) =
			runtime.batch_message_channel(config.max_queue_size);
		let ticker = runtime
			.interval(config.scheduled_delay)
			.map(|_| BatchMessage::Flush(None));
		let timeout_runtime = runtime.clone();

		let messages = Box::pin(stream::select(message_receiver, ticker));
		let processor = BatchLogProcessorInternal {
			logs: Vec::new(),
			export_tasks: FuturesUnordered::new(),
			runtime: timeout_runtime,
			config,
			exporter,
		};

		// Spawn worker process via user-defined spawn function.
		runtime.spawn(Box::pin(processor.run(messages)));

		// Return batch processor with link to worker
		BatchLogProcessor { message_sender }
	}

	/// Create a new batch processor builder
	pub fn builder<E>(exporter: E, runtime: R) -> BatchLogProcessorBuilder<E, R>
	where
		E: LogExporter,
	{
		BatchLogProcessorBuilder {
			exporter,
			config: BatchConfig::default(),
			runtime,
		}
	}
}

/// Batch log processor configuration
#[derive(Debug)]
pub struct BatchConfig {
	/// The maximum queue size to buffer logs for delayed processing. If the
	/// queue gets full it drops the logs. The default value of is 2048.
	max_queue_size: usize,

	/// The delay interval in milliseconds between two consecutive processing
	/// of batches. The default value is 1 second.
	scheduled_delay: Duration,

	/// The maximum number of logs to process in a single batch. If there are
	/// more than one batch worth of logs then it processes multiple batches
	/// of logs one batch after the other without any delay. The default value
	/// is 512.
	max_export_batch_size: usize,

	/// The maximum duration to export a batch of data.
	max_export_timeout: Duration,

	/// Maximum number of concurrent exports
	///
	/// Limits the number of spawned tasks for exports and thus memory consumed
	/// by an exporter. A value of 1 will cause exports to be performed
	/// synchronously on the BatchSpanProcessor task.
	max_concurrent_exports: usize,
}

impl Default for BatchConfig {
	fn default() -> Self {
		let mut config = BatchConfig {
			max_queue_size: 2048,
			scheduled_delay: Duration::from_millis(1_000),
			max_export_batch_size: 512,
			max_export_timeout: Duration::from_millis(30_000),
			max_concurrent_exports: 1,
		};

		if let Some(max_concurrent_exports) = env::var("OTEL_BLRP_MAX_CONCURRENT_EXPORTS")
			.ok()
			.and_then(|max_concurrent_exports| usize::from_str(&max_concurrent_exports).ok())
		{
			config.max_concurrent_exports = max_concurrent_exports;
		}

		if let Some(max_queue_size) = env::var("OTEL_BLRP_MAX_QUEUE_SIZE")
			.ok()
			.and_then(|queue_size| usize::from_str(&queue_size).ok())
		{
			config.max_queue_size = max_queue_size;
		}

		if let Some(scheduled_delay) = env::var("OTEL_BLRP_SCHEDULE_DELAY")
			.ok()
			.or_else(|| env::var("OTEL_BSP_SCHEDULE_DELAY_MILLIS").ok())
			.and_then(|delay| u64::from_str(&delay).ok())
		{
			config.scheduled_delay = Duration::from_millis(scheduled_delay);
		}

		if let Some(max_export_batch_size) = env::var("OTEL_BLRP_MAX_EXPORT_BATCH_SIZE")
			.ok()
			.and_then(|batch_size| usize::from_str(&batch_size).ok())
		{
			config.max_export_batch_size = max_export_batch_size;
		}

		// max export batch size must be less or equal to max queue size.
		// we set max export batch size to max queue size if it's larger than max queue size.
		if config.max_export_batch_size > config.max_queue_size {
			config.max_export_batch_size = config.max_queue_size;
		}

		if let Some(max_export_timeout) = env::var("OTEL_BLRP_EXPORT_TIMEOUT")
			.ok()
			.or_else(|| env::var("OTEL_BSP_EXPORT_TIMEOUT_MILLIS").ok())
			.and_then(|timeout| u64::from_str(&timeout).ok())
		{
			config.max_export_timeout = Duration::from_millis(max_export_timeout);
		}

		config
	}
}

/// A builder for creating [`BatchLogProcessor`] instances.
///
#[derive(Debug)]
pub struct BatchLogProcessorBuilder<E, R> {
	exporter: E,
	config: BatchConfig,
	runtime: R,
}

impl<E, R> BatchLogProcessorBuilder<E, R>
where
	E: LogExporter + 'static,
	R: RuntimeChannel<BatchMessage>,
{
	/// Set max queue size for batches
	pub fn with_max_queue_size(self, size: usize) -> Self {
		let mut config = self.config;
		config.max_queue_size = size;

		BatchLogProcessorBuilder { config, ..self }
	}

	/// Set scheduled delay for batches
	pub fn with_scheduled_delay(self, delay: Duration) -> Self {
		let mut config = self.config;
		config.scheduled_delay = delay;

		BatchLogProcessorBuilder { config, ..self }
	}

	/// Set max timeout for exporting.
	pub fn with_max_timeout(self, timeout: Duration) -> Self {
		let mut config = self.config;
		config.max_export_timeout = timeout;

		BatchLogProcessorBuilder { config, ..self }
	}

	/// Set max export size for batches, should always less than or equals to max queue size.
	///
	/// If input is larger than max queue size, will lower it to be equal to max queue size
	pub fn with_max_export_batch_size(self, size: usize) -> Self {
		let mut config = self.config;
		if size > config.max_queue_size {
			config.max_export_batch_size = config.max_queue_size;
		} else {
			config.max_export_batch_size = size;
		}

		BatchLogProcessorBuilder { config, ..self }
	}

	/// Build a batch processor
	pub fn build(self) -> BatchLogProcessor<R> {
		BatchLogProcessor::new(Box::new(self.exporter), self.config, self.runtime)
	}
}

/// Messages sent between application thread and batch log processor's work thread.
#[allow(clippy::large_enum_variant)]
#[derive(Debug)]
pub enum BatchMessage {
	/// Export logs, usually called when the log is emitted.
	ExportLog(LogData),
	/// Flush the current buffer to the backend, it can be triggered by
	/// pre configured interval or a call to `force_push` function.
	Flush(Option<oneshot::Sender<ExportResult>>),
	/// Shut down the worker thread, push all logs in buffer to the backend.
	Shutdown(oneshot::Sender<ExportResult>),
}
