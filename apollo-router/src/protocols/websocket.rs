use std::pin::Pin;
use std::task::Poll;

use futures::future;
use futures::Future;
use futures::Sink;
use futures::SinkExt;
use futures::Stream;
use futures::StreamExt;
use pin_project_lite::pin_project;
use serde::Deserialize;
use serde::Serialize;
use tokio::io::AsyncRead;
use tokio::io::AsyncWrite;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::WebSocketStream;
use uuid::Uuid;

use crate::graphql;

// TODO use graphql::Error everywhere ?!

/// A websocket message received from the client
#[derive(Serialize, Debug)]
#[serde(tag = "type", rename_all = "snake_case")]
#[allow(clippy::large_enum_variant)] // Request is at fault
pub(crate) enum ClientMessage {
    /// A new connection
    ConnectionInit {
        /// Optional init payload from the client
        payload: Option<serde_json_bytes::Value>,
    },
    /// The start of a Websocket subscription
    #[serde(alias = "start")]
    Subscribe {
        /// Message ID
        id: String,
        /// The GraphQL Request - this can be modified by protocol implementors
        /// to add files uploads.
        payload: graphql::Request,
    },
    /// The end of a Websocket subscription
    #[serde(alias = "stop")]
    Complete {
        /// Message ID
        id: String,
    },
    /// Connection terminated by the client
    ConnectionTerminate,
    /// Useful for detecting failed connections, displaying latency metrics or
    /// other types of network probing.
    ///
    /// Reference: <https://github.com/enisdenjo/graphql-ws/blob/master/PROTOCOL.md#ping>
    Ping {
        /// Additional details about the ping.
        #[serde(skip_serializing_if = "Option::is_none")]
        payload: Option<serde_json_bytes::Value>,
    },
    /// The response to the Ping message.
    ///
    /// Reference: <https://github.com/enisdenjo/graphql-ws/blob/master/PROTOCOL.md#pong>
    Pong {
        /// Additional details about the pong.
        #[serde(skip_serializing_if = "Option::is_none")]
        payload: Option<serde_json_bytes::Value>,
    },
}

#[derive(Deserialize, Debug)]
#[serde(tag = "type", rename_all = "snake_case")]
pub(crate) enum ServerMessage {
    ConnectionAck,
    /// subscriptions-transport-ws protocol next payload
    Data {
        id: String,
        payload: graphql::Response,
    },
    /// graphql-ws protocol next payload
    Next {
        id: String,
        payload: graphql::Response,
    },
    Error {
        id: String,
        payload: Vec<graphql::Error>,
    },
    Complete {
        id: String,
    },
    /// The response to the Ping message.
    ///
    /// https://github.com/enisdenjo/graphql-ws/blob/master/PROTOCOL.md#pong
    Pong {
        payload: Option<serde_json::Value>,
    },
    Ping {
        payload: Option<serde_json::Value>,
    },
}

impl ServerMessage {
    fn into_graphql_response(self) -> (Option<graphql::Response>, bool) {
        match self {
            ServerMessage::Next { id, payload } | ServerMessage::Data { id, payload } => {
                (Some(payload), false)
            }
            ServerMessage::Error { id, payload } => (
                Some(graphql::Response::builder().errors(payload).build()),
                true,
            ),
            ServerMessage::Complete { .. } => (None, true),
            ServerMessage::ConnectionAck | ServerMessage::Pong { .. } => (None, false),
            ServerMessage::Ping { .. } => (None, false),
        }
    }
}

pin_project! {
pub(crate) struct GraphqlWebSocket<S> {
    #[pin]
    stream: S,
    id: Uuid,
}
}

impl<S> GraphqlWebSocket<S>
where
    S: Stream<Item = serde_json::Result<ServerMessage>> + Sink<ClientMessage> + std::marker::Unpin,
{
    pub(crate) async fn new(
        mut stream: S,
        id: Uuid,
    ) -> Result<Self, <S as Sink<ClientMessage>>::Error> {
        stream
            .send(ClientMessage::ConnectionInit { payload: None })
            .await?;
        Ok(Self { stream, id })
    }
}

pub(crate) fn convert_websocket_stream<T>(
    stream: WebSocketStream<T>,
    id: Uuid,
) -> impl Stream<Item = serde_json::Result<ServerMessage>>
       + Sink<ClientMessage, Error = tokio_tungstenite::tungstenite::Error>
where
    T: AsyncRead + AsyncWrite + Unpin,
{
    stream
        .with(|client_message: ClientMessage| {
            future::ready(Ok::<_, tokio_tungstenite::tungstenite::Error>(
                Message::Text(serde_json::to_string(&client_message).unwrap()),
            ))
        })
        .take_while(|res| future::ready(res.is_ok())) // TODO log error or something
        .map(Result::unwrap)
        .map(move |msg| match msg {
            Message::Text(text) => serde_json::from_str(&text),
            Message::Binary(bin) => serde_json::from_slice(&bin),
            Message::Ping(payload) => Ok(ServerMessage::Ping {
                payload: serde_json::from_slice(&payload).ok(),
            }),
            Message::Pong(payload) => Ok(ServerMessage::Pong {
                payload: serde_json::from_slice(&payload).ok(),
            }),
            Message::Close(_payload) => Ok(ServerMessage::Complete { id: id.to_string() }),
            Message::Frame(frame) => serde_json::from_slice(frame.payload()),
        })
}

impl<S> Stream for GraphqlWebSocket<S>
where
    S: Stream<Item = serde_json::Result<ServerMessage>> + Sink<ClientMessage>,
{
    type Item = graphql::Response;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        let mut this = self.as_mut().project();

        match Pin::new(&mut this.stream).poll_next(cx) {
            Poll::Ready(message) => match message {
                Some(server_message) => match server_message {
                    Ok(server_message) => {
                        if let ServerMessage::Ping { .. } = server_message {
                            // Send pong asynchronously
                            let _ = Pin::new(
                                &mut Pin::new(&mut this.stream)
                                    .send(ClientMessage::Pong { payload: None }),
                            )
                            .poll(cx);
                        }
                        match server_message.into_graphql_response() {
                            (None, true) => Poll::Ready(None),
                            // For ignored message like ACK, Ping, Pong, etc...
                            (None, false) => self.poll_next(cx),
                            (Some(resp), _) => Poll::Ready(Some(resp)),
                        }
                    }
                    Err(err) => Poll::Ready(
                        graphql::Response::builder()
                            .error(
                                graphql::Error::builder()
                                    .message(format!(
                                        "cannot deserialize websocket server message: {err:?}"
                                    ))
                                    .extension_code("INVALID_WEBSOCKET_SERVER_MESSAGE_FORMAT")
                                    .build(),
                            )
                            .build()
                            .into(),
                    ),
                },
                None => Poll::Ready(None),
            },
            Poll::Pending => Poll::Pending,
        }
    }
}

impl<S> Sink<graphql::Request> for GraphqlWebSocket<S>
where
    S: Stream<Item = serde_json::Result<ServerMessage>> + Sink<ClientMessage>,
{
    type Error = graphql::Error;

    fn poll_ready(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Result<(), Self::Error>> {
        let mut this = self.project();

        match Pin::new(&mut this.stream).poll_ready(cx) {
            Poll::Ready(Ok(_)) => Poll::Ready(Ok(())),
            Poll::Ready(Err(_err)) => Poll::Ready(Err("websocket connection error")),
            Poll::Pending => Poll::Pending,
        }
        .map_err(|err| {
            graphql::Error::builder()
                .message(format!("cannot establish websocket connection: {err}"))
                .extension_code("WEBSOCKET_CONNECTION_ERROR")
                .build()
        })
    }

    fn start_send(self: Pin<&mut Self>, item: graphql::Request) -> Result<(), Self::Error> {
        let mut this = self.project();

        Pin::new(&mut this.stream)
            .start_send(ClientMessage::Subscribe {
                payload: item,
                id: this.id.to_string(),
            })
            .map_err(|_err| {
                graphql::Error::builder()
                    .message("cannot send to websocket connection")
                    .extension_code("WEBSOCKET_CONNECTION_ERROR")
                    .build()
            })
    }

    fn poll_flush(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Result<(), Self::Error>> {
        let mut this = self.project();
        Pin::new(&mut this.stream).poll_flush(cx).map_err(|_err| {
            graphql::Error::builder()
                .message("cannot flush to websocket connection")
                .extension_code("WEBSOCKET_CONNECTION_ERROR")
                .build()
        })
    }

    fn poll_close(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Result<(), Self::Error>> {
        let mut this = self.project();
        match Pin::new(
            &mut Pin::new(&mut this.stream).send(ClientMessage::Complete {
                id: this.id.to_string(),
            }),
        )
        .poll(cx)
        {
            Poll::Ready(_) => {}
            Poll::Pending => {
                return Poll::Pending;
            }
        }
        Pin::new(&mut this.stream).poll_close(cx).map_err(|_err| {
            graphql::Error::builder()
                .message("cannot close websocket connection")
                .extension_code("WEBSOCKET_CONNECTION_ERROR")
                .build()
        })
    }
}

#[derive(Deserialize, Serialize)]
struct WithId {
    id: String,
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use futures::future;
    use futures::StreamExt;
    use http::HeaderValue;
    use tokio_tungstenite::connect_async;
    use tokio_tungstenite::tungstenite::client::IntoClientRequest;
    use tokio_tungstenite::tungstenite::Message;

    use super::*;

    #[ignore]
    #[tokio::test]
    async fn test_ws_connection() {
        let url = url::Url::parse("ws://localhost:4041/ws").unwrap();
        let mut request = url.into_client_request().unwrap();
        request.headers_mut().insert(
            http::header::SEC_WEBSOCKET_PROTOCOL,
            // New one
            HeaderValue::from_static("graphql-transport-ws"),
            // Old one
            // HeaderValue::from_static("graphql-ws"),
        );
        let (ws_stream, resp) = connect_async(request).await.unwrap();

        let sub_uuid = Uuid::new_v4();
        let gql_stream =
            GraphqlWebSocket::new(convert_websocket_stream(ws_stream, sub_uuid), sub_uuid)
                .await
                .unwrap();

        let sub = r#"subscription {
          userWasCreated {
            username
          }
        }"#;
        let (mut gql_sink, mut gql_read_stream) = gql_stream.split();
        let handle = tokio::task::spawn(async move {
            gql_sink
                .send(graphql::Request::builder().query(sub).build())
                .await
                .unwrap();
        });

        while let Some(msg) = gql_read_stream.next().await {
            dbg!(msg);
        }
    }
}
