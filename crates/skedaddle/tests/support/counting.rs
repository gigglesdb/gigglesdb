//! Request instrumentation only: all storage operations reach the real backend.
use futures::stream::BoxStream;
use object_store::{ObjectStore, path::Path, *};
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicUsize, Ordering},
};

#[derive(Debug)]
pub struct CountingStore {
    inner: Arc<dyn ObjectStore>,
    gets: AtomicUsize,
    lists: AtomicUsize,
    barrier: Mutex<Option<Arc<PutBarrier>>>,
    next_put: Mutex<Option<Arc<BlockedPut>>>,
}
#[derive(Debug)]
struct PutBarrier {
    barrier: tokio::sync::Barrier,
    remaining: AtomicUsize,
}
#[derive(Debug, Default)]
pub struct BlockedPut {
    entered: tokio::sync::Notify,
    release: tokio::sync::Notify,
}
impl BlockedPut {
    pub async fn entered(&self) {
        self.entered.notified().await;
    }
    pub fn release(&self) {
        self.release.notify_one();
    }
}
impl CountingStore {
    pub fn new(inner: Arc<dyn ObjectStore>) -> Self {
        Self {
            inner,
            gets: AtomicUsize::new(0),
            lists: AtomicUsize::new(0),
            barrier: Mutex::new(None),
            next_put: Mutex::new(None),
        }
    }
    pub fn reset(&self) {
        self.gets.store(0, Ordering::Relaxed);
        self.lists.store(0, Ordering::Relaxed);
    }
    pub fn counts(&self) -> (usize, usize) {
        (
            self.gets.load(Ordering::Relaxed),
            self.lists.load(Ordering::Relaxed),
        )
    }
    pub fn arm_put_barrier(&self, participants: usize) {
        *self.barrier.lock().unwrap() = Some(Arc::new(PutBarrier {
            barrier: tokio::sync::Barrier::new(participants),
            remaining: AtomicUsize::new(participants),
        }));
    }
    pub fn block_next_put(&self) -> Arc<BlockedPut> {
        let gate = Arc::new(BlockedPut::default());
        *self.next_put.lock().unwrap() = Some(gate.clone());
        gate
    }
}
impl std::fmt::Display for CountingStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "CountingStore({})", self.inner)
    }
}
#[async_trait::async_trait]
impl ObjectStore for CountingStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        options: PutOptions,
    ) -> object_store::Result<PutResult> {
        let blocked = { self.next_put.lock().unwrap().take() };
        if let Some(blocked) = blocked {
            blocked.entered.notify_one();
            blocked.release.notified().await;
        }
        let barrier = { self.barrier.lock().unwrap().clone() };
        if let Some(barrier) = barrier
            && barrier
                .remaining
                .try_update(Ordering::AcqRel, Ordering::Acquire, |n| n.checked_sub(1))
                .is_ok()
        {
            barrier.barrier.wait().await;
        }
        self.inner.put_opts(location, payload, options).await
    }
    async fn put_multipart_opts(
        &self,
        location: &Path,
        options: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, options).await
    }
    async fn get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        self.gets.fetch_add(1, Ordering::Relaxed);
        self.inner.get_opts(location, options).await
    }
    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.lists.fetch_add(1, Ordering::Relaxed);
        self.inner.list(prefix)
    }
    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.lists.fetch_add(1, Ordering::Relaxed);
        self.inner.list_with_offset(prefix, offset)
    }
    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<ListResult> {
        self.lists.fetch_add(1, Ordering::Relaxed);
        self.inner.list_with_delimiter(prefix).await
    }
    fn delete_stream(
        &self,
        locations: BoxStream<'static, object_store::Result<Path>>,
    ) -> BoxStream<'static, object_store::Result<Path>> {
        self.inner.delete_stream(locations)
    }
    async fn copy_opts(
        &self,
        from: &Path,
        to: &Path,
        options: CopyOptions,
    ) -> object_store::Result<()> {
        self.inner.copy_opts(from, to, options).await
    }
    async fn rename_opts(
        &self,
        from: &Path,
        to: &Path,
        options: RenameOptions,
    ) -> object_store::Result<()> {
        self.inner.rename_opts(from, to, options).await
    }
}
