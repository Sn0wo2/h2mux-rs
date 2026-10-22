use std::future::Future;
use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};

use bytes::Bytes;
use futures_util::StreamExt;
use futures_util::future::poll_fn;
use futures_util::stream::FuturesUnordered;
use http::{Request, Response, StatusCode};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

type AcceptedRequest = (Request<h2::RecvStream>, h2::server::SendResponse<Bytes>);
type AcceptResult = Option<Result<Box<AcceptedRequest>, h2::Error>>;
type StreamTask = Pin<Box<dyn Future<Output = io::Result<()>>>>;

enum DriverEvent {
    Accepted(AcceptResult),
    Stream(io::Result<()>),
}

pub struct H2Stream {
    receive: h2::RecvStream,
    send: h2::SendStream<Bytes>,
    read_buffer: Option<Bytes>,
    read_eof: bool,
    write_closed: bool,
}

impl std::fmt::Debug for H2Stream {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.debug_struct("H2Stream").finish_non_exhaustive()
    }
}

impl AsyncRead for H2Stream {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.as_mut().get_mut();
        if buf.remaining() == 0 {
            return Poll::Ready(Ok(()));
        }
        if this.read_buffer.is_none() && !this.read_eof {
            loop {
                let data = {
                    let mut data = std::pin::pin!(this.receive.data());
                    match data.as_mut().poll(cx) {
                        Poll::Pending => return Poll::Pending,
                        Poll::Ready(Some(Ok(data))) => Some(data),
                        Poll::Ready(Some(Err(error))) => {
                            return Poll::Ready(Err(io::Error::other(error.to_string())));
                        }
                        Poll::Ready(None) => None,
                    }
                };
                match data {
                    Some(data) if data.is_empty() => {}
                    Some(data) => {
                        this.read_buffer = Some(data);
                        break;
                    }
                    None => {
                        this.read_eof = true;
                        break;
                    }
                }
            }
        }
        let Some(data) = this.read_buffer.take() else {
            return Poll::Ready(Ok(()));
        };
        let length = data.len().min(buf.remaining());
        let Some(bytes) = data.get(..length) else {
            return Poll::Ready(Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "invalid h2mux read length",
            )));
        };
        buf.put_slice(bytes);
        if length < data.len() {
            this.read_buffer = Some(data.slice(length..));
        }
        if length > 0 {
            this.receive
                .flow_control()
                .release_capacity(length)
                .map_err(|error| io::Error::other(error.to_string()))?;
        }
        Poll::Ready(Ok(()))
    }
}

impl AsyncWrite for H2Stream {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let this = self.as_mut().get_mut();
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
        let capacity = match this.send.poll_capacity(cx) {
            Poll::Pending => return Poll::Pending,
            Poll::Ready(Some(Ok(capacity))) => capacity,
            Poll::Ready(Some(Err(error))) => {
                return Poll::Ready(Err(io::Error::other(error.to_string())));
            }
            Poll::Ready(None) => {
                return Poll::Ready(Err(io::Error::new(
                    io::ErrorKind::BrokenPipe,
                    "h2mux response stream closed",
                )));
            }
        };
        if capacity == 0 {
            return Poll::Pending;
        }
        let length = capacity.min(buf.len());
        let Some(bytes) = buf.get(..length) else {
            return Poll::Ready(Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "invalid h2mux write length",
            )));
        };
        this.send
            .send_data(Bytes::copy_from_slice(bytes), false)
            .map_err(|error| io::Error::other(error.to_string()))?;
        Poll::Ready(Ok(length))
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.as_mut().get_mut();
        if this.write_closed {
            return Poll::Ready(Ok(()));
        }
        this.send
            .send_data(Bytes::new(), true)
            .map_err(|error| io::Error::other(error.to_string()))?;
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
    let mut builder = h2::server::Builder::new();
    builder.max_concurrent_streams(u32::try_from(max_streams).unwrap_or(u32::MAX));
    let mut connection = builder
        .handshake::<_, Bytes>(io)
        .await
        .map_err(|error| io::Error::other(error.to_string()))?;
    let mut stream_tasks = FuturesUnordered::<StreamTask>::new();

    loop {
        let event = poll_fn(|cx| {
            if !stream_tasks.is_empty()
                && let Poll::Ready(Some(result)) = stream_tasks.poll_next_unpin(cx)
            {
                return Poll::Ready(DriverEvent::Stream(result));
            }
            if let Poll::Ready(accepted) = connection.poll_accept(cx) {
                return Poll::Ready(DriverEvent::Accepted(
                    accepted.map(|result| result.map(Box::new)),
                ));
            }
            Poll::Pending
        })
        .await;

        match event {
            DriverEvent::Stream(Ok(())) => {}
            DriverEvent::Stream(Err(error)) => {
                tracing::warn!("h2mux: stream failed: {error:?}");
            }
            DriverEvent::Accepted(Some(Ok(accepted))) => {
                let (request, mut respond) = *accepted;
                if stream_tasks.len() >= max_streams {
                    respond.send_reset(h2::Reason::REFUSED_STREAM);
                    continue;
                }
                let mut response = Response::new(());
                *response.status_mut() = StatusCode::OK;
                let send = respond
                    .send_response(response, false)
                    .map_err(|error| io::Error::other(error.to_string()))?;
                let receive = request.into_body();
                let handler = handler.clone();
                stream_tasks.push(Box::pin(async move {
                    handler(H2Stream {
                        receive,
                        send,
                        read_buffer: None,
                        read_eof: false,
                        write_closed: false,
                    })
                    .await
                }));
            }
            DriverEvent::Accepted(Some(Err(error))) => {
                return Err(io::Error::other(error.to_string()));
            }
            DriverEvent::Accepted(None) => return Ok(()),
        }
    }
}

pub fn error_response(message: &str) -> Vec<u8> {
    let mut payload = vec![0x01];
    let mut length = message.len() as u64;
    loop {
        let byte = (length & 0x7f) as u8;
        length >>= 7;
        payload.push(if length == 0 { byte } else { byte | 0x80 });
        if length == 0 {
            break;
        }
    }
    payload.extend_from_slice(message.as_bytes());
    payload
}
