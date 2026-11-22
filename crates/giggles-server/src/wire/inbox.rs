//! Bounded read-ahead and cancellation for a native-wire connection.

use super::{
    conn::{LOOKAHEAD_BYTES, MAX_BATCH_ENTRIES},
    frame::{Direction, FrameDecoder, FrameHeader, FrameKind, HEADER_LEN},
    messages::WireCancel,
};
use crate::MAX_FRAME;
use bytes::BufMut;
use giggles_core::Cancellation;
use prost::{
    Message,
    encoding::{DecodeContext, WireType, decode_key, skip_field},
};
use std::{
    collections::VecDeque,
    io,
    sync::{
        Arc, Mutex, MutexGuard, PoisonError,
        atomic::{AtomicUsize, Ordering::Relaxed},
    },
};
use tokio::{
    io::{AsyncRead, AsyncReadExt},
    sync::Notify,
    task::JoinHandle,
};

#[cfg(any(test, feature = "test-hooks"))]
use super::conn::ConnectionStats;

const READ_CHUNK: usize = 8 * 1024;

pub(super) enum Inbound {
    Frame {
        header: FrameHeader,
        body: Vec<u8>,
        cancel_observation: Option<CancelObservation>,
        request_slots: usize,
    },
    Eof,
    Io(io::Error),
    Protocol(&'static str),
}

pub(super) enum CancelObservation {
    Running,
    Early(u32),
}

struct Queue {
    items: VecDeque<Inbound>,
    bytes: usize,
    partial: usize,
    // Keep a popped frame charged until the connection has published what
    // it deferred from that frame.
    popped_bytes: usize,
    // The connection publishes the admitted slots plus remaining lookahead
    // slots. Like the old decoder, the reader may finish its current chunk.
    read_frames: usize,
    available: usize,
}

impl Queue {
    fn read_limit(&self, deferred_bytes: usize) -> usize {
        if self.items.len() + usize::from(self.popped_bytes != 0) >= self.read_frames {
            return 0;
        }
        // Requests that fit into the connection's free slots would already
        // be in its admitted queue if the connection were polling us.
        let mut available = self.available;
        let mut bytes = self.bytes;
        for item in &self.items {
            if available == 0 {
                break;
            }
            let Inbound::Frame {
                header,
                request_slots,
                ..
            } = item
            else {
                break;
            };
            if *request_slots == 0 {
                continue;
            }
            // Charge the whole of a partially deferred batch until the
            // connection can account for its deferred entries individually.
            if *request_slots > available {
                break;
            }
            available -= request_slots;
            bytes -= HEADER_LEN + header.body_len;
        }
        READ_CHUNK.min(
            LOOKAHEAD_BYTES
                .saturating_sub(deferred_bytes + self.partial + bytes + self.popped_bytes),
        )
    }
}

#[derive(Default)]
struct Cancels {
    running: Option<(u32, Cancellation)>,
    early: Vec<u32>,
    closed: bool,
}

pub(super) struct Inbox {
    queue: Mutex<Queue>,
    deferred_bytes: AtomicUsize,
    consumer: Notify,
    producer: Notify,
    cancels: Mutex<Cancels>,
    #[cfg(any(test, feature = "test-hooks"))]
    stats: Arc<ConnectionStats>,
}

impl Inbox {
    // A reader that panics under a lock still ends the connection through
    // ReaderExit, so both sides keep using the state behind a poisoned lock.
    fn queue(&self) -> MutexGuard<'_, Queue> {
        self.queue.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn cancels(&self) -> MutexGuard<'_, Cancels> {
        self.cancels.lock().unwrap_or_else(PoisonError::into_inner)
    }

    pub(super) fn start<R: AsyncRead + Unpin + Send + 'static>(
        reader: R,
        available: usize,
        #[cfg(any(test, feature = "test-hooks"))] stats: Arc<ConnectionStats>,
    ) -> ReaderTask {
        let inbox = Arc::new(Self {
            queue: Mutex::new(Queue {
                items: VecDeque::new(),
                bytes: 0,
                partial: 0,
                popped_bytes: 0,
                read_frames: available.saturating_mul(2),
                available,
            }),
            deferred_bytes: AtomicUsize::new(0),
            consumer: Notify::new(),
            producer: Notify::new(),
            cancels: Mutex::default(),
            #[cfg(any(test, feature = "test-hooks"))]
            stats,
        });
        let source = inbox.clone();
        #[cfg(test)]
        let count = ReaderCount::new(inbox.stats.clone());
        let task = tokio::spawn(async move {
            #[cfg(test)]
            let _count = count;
            let _exit = ReaderExit(&source);
            source.read(reader).await;
        });
        ReaderTask { inbox, task }
    }

    // Publish released and deferred bytes in the same critical section as
    // the pop, so the reader cannot spend the same byte allowance twice.
    pub(super) fn advance(
        &self,
        deferred: usize,
        available: usize,
        read_frames: usize,
        pop: bool,
    ) -> Option<Inbound> {
        let mut queue = self.queue();
        let old_limit = queue.read_limit(self.deferred_bytes.load(Relaxed));
        self.deferred_bytes.store(deferred, Relaxed);
        queue.available = available;
        queue.read_frames = read_frames;
        queue.popped_bytes = 0;
        let item = if pop { queue.items.pop_front() } else { None };
        if let Some(Inbound::Frame {
            header,
            request_slots,
            ..
        }) = &item
        {
            queue.available = available.saturating_sub(*request_slots);
            queue.popped_bytes = HEADER_LEN + header.body_len;
            queue.bytes -= queue.popped_bytes;
        }
        let limit = queue.read_limit(deferred);
        #[cfg(test)]
        self.record_decoder_len(&queue);
        drop(queue);
        if limit > old_limit || (limit == 0 && old_limit != 0) {
            self.producer.notify_one();
        }
        item
    }

    pub(super) async fn wait(&self) {
        loop {
            let notified = self.consumer.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if !self.queue().items.is_empty() {
                return;
            }
            notified.await;
        }
    }

    pub(super) fn register(&self, id: u32, cancel: Cancellation) {
        let mut cancels = self.cancels();
        if cancels.closed || cancels.early.contains(&id) {
            cancel.cancel();
        }
        cancels.running = Some((id, cancel));
    }

    pub(super) fn clear_running(&self) {
        self.cancels().running = None;
    }

    pub(super) fn consume_early_cancel(&self, target: u32) {
        let mut cancels = self.cancels();
        if let Some(index) = cancels.early.iter().position(|id| *id == target) {
            cancels.early.swap_remove(index);
        }
    }

    fn cancel(&self, target: u32) -> CancelObservation {
        let mut cancels = self.cancels();
        if let Some((_, cancel)) = cancels.running.as_ref().filter(|(id, _)| *id == target) {
            cancel.cancel();
            CancelObservation::Running
        } else {
            cancels.early.push(target);
            CancelObservation::Early(target)
        }
    }

    fn close_reader(&self, terminal: Inbound) {
        let mut cancels = self.cancels();
        cancels.closed = true;
        if let Some((_, cancel)) = &cancels.running {
            cancel.cancel();
        }
        drop(cancels);

        let mut queue = self.queue();
        queue.partial = 0;
        queue.items.push_back(terminal);
        #[cfg(test)]
        self.record_decoder_len(&queue);
        drop(queue);
        self.consumer.notify_one();
    }

    async fn read(&self, mut reader: impl AsyncRead + Unpin) {
        let mut decoder = FrameDecoder::new(Direction::ClientToServer, MAX_FRAME);
        loop {
            let notified = self.producer.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            let limit = self
                .queue()
                .read_limit(self.deferred_bytes.load(Relaxed));
            if limit == 0 {
                notified.await;
                continue;
            }
            let mut buf = decoder.read_buf().limit(limit);
            let result = tokio::select! {
                biased;
                _ = notified => continue,
                result = reader.read_buf(&mut buf) => result,
            };
            match result {
                Ok(0) => {
                    self.close_reader(Inbound::Eof);
                    return;
                }
                Err(error) => {
                    self.close_reader(Inbound::Io(error));
                    return;
                }
                Ok(_bytes) => {
                    #[cfg(test)]
                    self.stats.read_bytes.fetch_add(_bytes, Relaxed);
                    if !self.forward(&mut decoder) {
                        return;
                    }
                }
            }
        }
    }

    fn forward(&self, decoder: &mut FrameDecoder) -> bool {
        let mut queue = self.queue();
        let was_empty = queue.items.is_empty();
        #[cfg(feature = "test-hooks")]
        let mut received_frames = 0;
        #[cfg(test)]
        self.stats
            .peak_decoder_len
            .fetch_max(queue.bytes + decoder.read_buf().len(), Relaxed);
        let keep_reading = loop {
            let (header, body) = match decoder.next_frame() {
                Ok(Some(frame)) => frame,
                Ok(None) => break true,
                Err(_) => {
                    queue
                        .items
                        .push_back(Inbound::Protocol("invalid frame header"));
                    break false;
                }
            };
            let body = decoder.body(body).to_vec();
            let goodbye = header.kind == FrameKind::Goodbye && body.is_empty();
            let cancel_observation = if header.kind == FrameKind::Cancel
                && header.request_id != 0
                && header.flags.0 == 0
            {
                WireCancel::decode(body.as_slice())
                    .ok()
                    .map(|cancel| self.cancel(cancel.target_request_id))
            } else {
                None
            };
            let request_slots = match header.kind {
                FrameKind::Cancel | FrameKind::Goodbye => 0,
                FrameKind::Batch => batch_slots(&body).unwrap_or(usize::MAX),
                _ => 1,
            };
            queue.bytes += HEADER_LEN + body.len();
            queue.items.push_back(Inbound::Frame {
                header,
                body,
                cancel_observation,
                request_slots,
            });
            #[cfg(feature = "test-hooks")]
            {
                received_frames += 1;
            }
            if goodbye {
                break false;
            }
        };
        decoder.consume_decoded();
        queue.partial = if keep_reading {
            decoder.read_buf().len()
        } else {
            0
        };
        #[cfg(test)]
        self.record_decoder_len(&queue);
        let has_items = !queue.items.is_empty();
        #[cfg(feature = "test-hooks")]
        self.stats
            .received_frames
            .fetch_add(received_frames, Relaxed);
        drop(queue);
        if was_empty && has_items {
            self.consumer.notify_one();
        }
        keep_reading
    }

    #[cfg(test)]
    fn record_decoder_len(&self, queue: &Queue) {
        self.stats
            .decoder_len
            .store(queue.partial + queue.bytes, Relaxed);
    }
}

// Count top-level WireBatch.entries without allocating or decoding the
// requests while holding the inbox lock. The connection validates the batch.
fn batch_slots(mut body: &[u8]) -> Option<usize> {
    const ENTRIES_TAG: u32 = 2;
    let mut slots = 0;
    let context = DecodeContext::default();
    while !body.is_empty() {
        let (tag, wire_type) = decode_key(&mut body).ok()?;
        if tag == ENTRIES_TAG {
            if wire_type != WireType::LengthDelimited || slots == MAX_BATCH_ENTRIES {
                return None;
            }
            slots += 1;
        }
        skip_field(wire_type, tag, &mut body, context.clone()).ok()?;
    }
    Some(slots.max(1))
}

struct ReaderExit<'a>(&'a Inbox);

impl Drop for ReaderExit<'_> {
    fn drop(&mut self) {
        // Normal exits have already forwarded a terminal item. An explicit
        // abort is owned by ReaderTask and needs no reader error.
        if std::thread::panicking() {
            self.0
                .close_reader(Inbound::Io(io::Error::other("wire reader task ended")));
        }
    }
}

pub(super) struct ReaderTask {
    pub(super) inbox: Arc<Inbox>,
    task: JoinHandle<()>,
}

impl ReaderTask {
    pub(super) async fn stop(&mut self) {
        self.task.abort();
        let _ = (&mut self.task).await;
    }
}

impl Drop for ReaderTask {
    fn drop(&mut self) {
        self.task.abort();
        if let Some((_, cancel)) = self
            .inbox
            .cancels()
            .running
            .take()
        {
            cancel.cancel();
        }
    }
}

#[cfg(test)]
struct ReaderCount(Arc<ConnectionStats>);

#[cfg(test)]
impl ReaderCount {
    fn new(stats: Arc<ConnectionStats>) -> Self {
        stats.reader_tasks.fetch_add(1, Relaxed);
        Self(stats)
    }
}

#[cfg(test)]
impl Drop for ReaderCount {
    fn drop(&mut self) {
        self.0.reader_tasks.fetch_sub(1, Relaxed);
    }
}
