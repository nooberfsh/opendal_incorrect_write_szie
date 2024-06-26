use std::{
    pin::Pin,
    sync::{atomic::AtomicU64, Arc},
};

use anyhow::Result;
use futures::AsyncWrite;
use opendal::services::Fs;
use opendal::{FuturesAsyncWriter, Operator};
use tokio::io::AsyncWriteExt;

#[tokio::main]
async fn main() -> Result<()> {
    let mut w = make_writer().await?;

    let data = "hello, opendal";
    for _ in 0..1_000_000 {
        w.write_all(data.as_bytes()).await?
    }

    w.shutdown().await?;
    let wsize = w.written_size.load(std::sync::atomic::Ordering::SeqCst);
    println!("written size: {wsize}");

    Ok(())
}

async fn make_writer() -> Result<TrackWriter> {
    let mut builder = Fs::default();
    builder.root(".");
    let op: Operator = Operator::new(builder)?.finish();

    let w = op
        .writer("test.txt")
        .await
        .unwrap()
        .into_futures_async_write();
    Ok(TrackWriter::new(w))
}

/// `TrackWriter` is used to track the written size.
pub struct TrackWriter {
    writer: FuturesAsyncWriter,
    written_size: Arc<AtomicU64>,
}

impl TrackWriter {
    pub fn new(writer: FuturesAsyncWriter) -> Self {
        Self {
            writer,
            written_size: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Return a reference to the written size, it can be used to track the written size.
    pub fn get_wrriten_size(&self) -> Arc<AtomicU64> {
        self.written_size.clone()
    }
}

impl tokio::io::AsyncWrite for TrackWriter {
    fn poll_write(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<std::result::Result<usize, std::io::Error>> {
        match Pin::new(&mut self.writer).poll_write(cx, buf) {
            std::task::Poll::Ready(Ok(n)) => {
                self.written_size
                    .fetch_add(buf.len() as u64, std::sync::atomic::Ordering::SeqCst);
                std::task::Poll::Ready(Ok(n))
            }
            std::task::Poll::Ready(Err(e)) => std::task::Poll::Ready(Err(e)),
            std::task::Poll::Pending => std::task::Poll::Pending,
        }
    }

    fn poll_flush(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::result::Result<(), std::io::Error>> {
        Pin::new(&mut self.writer).poll_flush(cx)
    }

    fn poll_shutdown(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::result::Result<(), std::io::Error>> {
        Pin::new(&mut self.writer).poll_close(cx)
    }
}
