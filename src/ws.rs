use crate::web::State;
use futures_util::stream::{SplitSink, SplitStream};
use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::sync::broadcast::error::RecvError;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tokio::task::{AbortHandle, JoinHandle};
use warp::ws::{Message, WebSocket};

pub struct WsApiClient {
    state: Arc<State>,
    outbound: UnboundedSender<OutboundApiMessage>,
    listing_changes: Option<LiveHandle>,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
enum InboundApiMessage {
    Subscribe { channel: MessageChannel },
    Unsubscribe { channel: MessageChannel },
}

#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
enum OutboundApiMessage {
    Subscribed { channel: MessageChannel },
    Unsubscribed { channel: MessageChannel },
    ListingsRevisionChanged { revision: u64 },
    Err { message: String },
}

#[derive(Serialize, Deserialize, Copy, Clone, Debug)]
#[serde(rename_all = "snake_case")]
enum MessageChannel {
    ListingChanges,
}

impl WsApiClient {
    async fn handle(&mut self, msg: InboundApiMessage) {
        match msg {
            InboundApiMessage::Subscribe { channel } => {
                match channel {
                    MessageChannel::ListingChanges => {
                        self.listing_changes = Some(
                            tokio::spawn(Self::listing_changes_task(
                                self.state.clone(),
                                self.outbound.clone(),
                            ))
                            .into(),
                        )
                    }
                };

                // send a message letting the client know they've been subscribed
                self.outbound
                    .send(OutboundApiMessage::Subscribed { channel })
                    .unwrap()
            }
            InboundApiMessage::Unsubscribe { channel } => {
                match channel {
                    MessageChannel::ListingChanges => {
                        self.listing_changes = None; // drops the task.
                    }
                }

                // send a message letting the client know they've been unsubscribed
                self.outbound
                    .send(OutboundApiMessage::Unsubscribed { channel })
                    .unwrap()
            }
        }
    }

    pub async fn run(state: Arc<State>, web_socket: WebSocket) {
        let (outbound_sender, mut outbound_receiver) = tokio::sync::mpsc::unbounded_channel();
        let (mut ws_sender, mut ws_receiver) = web_socket.split();

        let mut client = Self {
            state,
            outbound: outbound_sender,
            listing_changes: None,
        };

        let send_task = Self::send_task(&mut outbound_receiver, &mut ws_sender);
        let recv_task = Self::recv_task(&mut ws_receiver, &mut client);

        // run either send or recv to completion;
        // either exiting is fatal to the ws client.
        tokio::select! {
            _ = send_task => (),
            _ = recv_task => (),
        }
    }

    async fn send_task(
        outbound_receiver: &mut UnboundedReceiver<OutboundApiMessage>,
        ws_sender: &mut SplitSink<WebSocket, Message>,
    ) {
        while let Some(msg) = outbound_receiver.recv().await {
            let Ok(json) = serde_json::to_string(&msg) else {
                tracing::debug!("failed to serialize outbound message: {:#?}", msg);
                continue;
            };

            if ws_sender.send(Message::text(json)).await.is_err() {
                break; // can't send. fatal. die
            }
        }
    }

    async fn recv_task(ws_receiver: &mut SplitStream<WebSocket>, client: &mut WsApiClient) {
        while let Some(Ok(msg)) = ws_receiver.next().await {
            // give up if there's an error (as far as I can tell they're fatal anyway)
            if let Ok(msg) = msg.to_str() {
                // only a close message has no to_str
                match serde_json::from_str::<InboundApiMessage>(msg) {
                    Ok(msg) => {
                        client.handle(msg).await;
                    }
                    Err(e) => {
                        let _ = client.outbound.send(OutboundApiMessage::Err {
                            message: e.to_string(),
                        });
                    }
                };
            }
        }
    }

    async fn listing_changes_task(state: Arc<State>, sender: UnboundedSender<OutboundApiMessage>) {
        let mut receiver = state.listings_change_channel.subscribe();
        let _ = sender.send(OutboundApiMessage::ListingsRevisionChanged {
            revision: state.current_listings_revision(),
        });

        loop {
            match receiver.recv().await {
                Ok(revision) => {
                    let _ = sender.send(OutboundApiMessage::ListingsRevisionChanged { revision });
                }
                Err(RecvError::Lagged(_)) => {
                    let _ = sender.send(OutboundApiMessage::ListingsRevisionChanged {
                        revision: state.current_listings_revision(),
                    });
                }
                Err(RecvError::Closed) => break,
            }
        }
    }
}

/// A handle to a tokio task that aborts the task when dropped.
struct LiveHandle(AbortHandle);

impl Drop for LiveHandle {
    fn drop(&mut self) {
        self.0.abort();
    }
}

impl<T> From<JoinHandle<T>> for LiveHandle {
    fn from(value: JoinHandle<T>) -> Self {
        Self(value.abort_handle())
    }
}
