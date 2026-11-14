use std::future::{Future, poll_fn};
use std::io;
use std::pin::Pin;
use std::task::{Context, Poll, ready};

use bytes::Bytes;
use futures_util::StreamExt;
use futures_util::stream::FuturesUnordered;
use http::Response;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

pub struct H2Stream {
    receive: h2::RecvStream,
    send: h2::SendStream<Bytes>,
    read_buffer: Bytes,
    write_closed: bool,
}

impl std::fmt::Debug for H2Stream {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.debug_struct("H2Stream").finish_non_exhaustive()
    }
}

impl AsyncRead for H2Stream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        if buf.remaining() == 0 {
            return Poll::Ready(Ok(()));
        }
        while this.read_buffer.is_empty() {
            match ready!(this.receive.poll_data(cx)) {
                Some(Ok(data)) => this.read_buffer = data,
                Some(Err(error)) => return Poll::Ready(Err(io::Error::other(error))),
                None => return Poll::Ready(Ok(())),
            }
        }
        let length = this.read_buffer.len().min(buf.remaining());
        buf.put_slice(&this.read_buffer.split_to(length));
        this.receive
            .flow_control()
            .release_capacity(length)
            .map_err(io::Error::other)?;
        Poll::Ready(Ok(()))
    }
}

impl AsyncWrite for H2Stream {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        if this.write_closed {
            return Poll::Ready(Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "h2mux response stream is closed",
            )));
        }
        if buf.is_empty() {
            return Poll::Ready(Ok(0));
        }
        this.send.reserve_capacity(buf.len());
        let length = match ready!(this.send.poll_capacity(cx)) {
            Some(Ok(capacity)) => capacity.min(buf.len()),
            Some(Err(error)) => return Poll::Ready(Err(io::Error::other(error))),
            None => {
                return Poll::Ready(Err(io::Error::new(
                    io::ErrorKind::BrokenPipe,
                    "h2mux response stream closed",
                )));
            }
        };
        this.send
            .send_data(Bytes::copy_from_slice(buf.split_at(length).0), false)
            .map_err(io::Error::other)?;
        Poll::Ready(Ok(length))
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        if this.write_closed {
            return Poll::Ready(Ok(()));
        }
        this.send
            .send_data(Bytes::new(), true)
            .map_err(io::Error::other)?;
        this.write_closed = true;
        Poll::Ready(Ok(()))
    }
}

pub async fn serve<I, C, F>(io: I, max_streams: usize, handler: C) -> io::Result<()>
where
    I: AsyncRead + AsyncWrite + Unpin,
    C: Fn(H2Stream) -> F + Clone + 'static,
    F: Future<Output = io::Result<()>> + 'static,
{
    let mut connection = h2::server::Builder::new()
        .max_concurrent_streams(u32::try_from(max_streams).unwrap_or(u32::MAX))
        .handshake::<_, Bytes>(io)
        .await
        .map_err(io::Error::other)?;
    let mut stream_tasks = FuturesUnordered::new();

    loop {
        match poll_fn(|cx| {
            while let Poll::Ready(Some(result)) = stream_tasks.poll_next_unpin(cx) {
                if let Err(error) = result {
                    tracing::warn!("h2mux: stream failed: {error:?}");
                }
            }
            connection.poll_accept(cx)
        })
        .await
        {
            Some(Ok((request, mut respond))) => {
                if stream_tasks.len() >= max_streams {
                    respond.send_reset(h2::Reason::REFUSED_STREAM);
                    continue;
                }
                let send = respond
                    .send_response(Response::new(()), false)
                    .map_err(io::Error::other)?;
                let handler = handler.clone();
                stream_tasks.push(async move {
                    handler(H2Stream {
                        receive: request.into_body(),
                        send,
                        read_buffer: Bytes::new(),
                        write_closed: false,
                    })
                    .await
                });
            }
            Some(Err(error)) => return Err(io::Error::other(error)),
            None => return Ok(()),
        }
    }
}

pub fn error_response(message: &str) -> Vec<u8> {
    [
        &[0x01],
        unsigned_varint::encode::usize(message.len(), &mut unsigned_varint::encode::usize_buffer()),
        message.as_bytes(),
    ]
    .concat()
}
