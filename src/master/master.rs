use crate::metadata::models::{Node, NodeUpdate};
use crate::metadata::sql::MetadataStorage;
use consulrs::api::service::common::{AgentService, AgentServiceChecksInfo};
use consulrs::api::ApiResponse;
use consulrs::client::ConsulClient;
use consulrs::error::ClientError;
use consulrs::service;
use std::collections::{HashMap, HashSet};
use std::time::Duration;
use tokio::sync::{broadcast, mpsc};
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

/// Tracks worker nodes and adjust operations to use only healthy ones.
/// Distributes chunks of files, maintains replication level of files.
pub struct Master {
    unhealthy_broadcast: broadcast::Sender<String>, // sends info about unhealthy worker node
    metadata: MetadataStorage,
    unhealthy_threshold: tokio::time::Duration,
}

impl Master {
    pub fn new(
        unhealthy_broadcast: broadcast::Sender<String>,
        metadata: MetadataStorage,
        unhealthy_threshold: tokio::time::Duration,
    ) -> Self {
        Self {
            unhealthy_broadcast,
            metadata,
            unhealthy_threshold,
        }
    }

    pub async fn run(
        &self,
        health_interval: Duration,
        consul_client: ConsulClient,
        cancellation_token: CancellationToken,
    ) {
        let (sender, receiver) = mpsc::channel(8);
        let actor = MasterActor::new(
            receiver,
            self.unhealthy_broadcast.clone(),
            self.metadata.clone(),
            consul_client,
            self.unhealthy_threshold,
        );
        tokio::spawn(run_master_actor(actor, cancellation_token.clone()));

        let mut health_interval = tokio::time::interval(health_interval);
        let mut chunks_check_interval = tokio::time::interval(Duration::from_secs(5));

        tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = health_interval.tick() => {
                        if let Err(err) = sender.send(MasterActorMessage::Health).await {
                            tracing::error!(err=format!("{}", err), "could not send message");
                        }
                    }
                    _ = chunks_check_interval.tick() => {
                        if let Err(err) = sender.send(MasterActorMessage::Chunks).await {
                            tracing::error!(err=format!("{}", err), "could not send message");
                        }
                    }
                    _ = cancellation_token.cancelled() => {
                        tracing::info!("closing master");
                        return;
                    }
                }
            }
        });
    }
}

struct MasterActor {
    receiver: mpsc::Receiver<MasterActorMessage>,
    unhealthy_broadcast: broadcast::Sender<String>, // sends info about unhealthy worker node
    consul_client: ConsulClient,
    metadata: MetadataStorage,
    unhealthy_threshold: tokio::time::Duration,

    worker_nodes: HashMap<String, tokio::time::Instant>,

    /// Set of all running nodes. It's cached value from the database, tracked in order no to
    /// update database each list call to consul.
    running_nodes: HashSet<String>,
}

enum MasterActorMessage {
    Health, // check workers health status form consul
    Chunks, // check if chunks' replication factor is met
}

impl MasterActor {
    pub fn new(
        receiver: mpsc::Receiver<MasterActorMessage>,
        unhealthy_broadcast: broadcast::Sender<String>,
        metadata: MetadataStorage,
        consul_client: ConsulClient,
        unhealthy_threshold: tokio::time::Duration,
    ) -> Self {
        Self {
            receiver,
            unhealthy_broadcast,
            metadata,
            consul_client,
            unhealthy_threshold,
            worker_nodes: HashMap::new(),
            running_nodes: HashSet::new(),
        }
    }

    async fn handle(&mut self, msg: MasterActorMessage) -> anyhow::Result<()> {
        match msg {
            MasterActorMessage::Health => self.handle_health().await,
            MasterActorMessage::Chunks => self.handle_chunks().await,
        }
    }

    async fn handle_health(&mut self) -> anyhow::Result<()> {
        let svcs = service::list(&self.consul_client, None).await?;
        for (name, s) in svcs.response {
            self.reconcile_consul_db(&name, s).await?;
            let healthy = check_if_worker_is_health(
                &name,
                service::health(&self.consul_client, &name, None).await,
            );

            if !healthy {
                let t = self
                    .worker_nodes
                    .entry(name.clone())
                    .or_insert(Instant::now());

                if t.elapsed() > self.unhealthy_threshold {
                    service::deregister(&self.consul_client, &name, None).await?;
                    self.metadata
                        .update_node(
                            &name,
                            NodeUpdate {
                                active: Some(false),
                            },
                        )
                        .await?;
                    tracing::info!(name = name, "unhealthy worker node unregistered");

                    // mark worker node as unhealthy
                    let _ = self.unhealthy_broadcast.send(name.clone());
                }
            }
        }
        Ok(())
    }

    async fn reconcile_consul_db(&mut self, node: &str, s: AgentService) -> anyhow::Result<()> {
        if self.running_nodes.contains(node) {
            return Ok(());
        }

        self.running_nodes.insert(node.to_string());
        self.metadata
            .save_node(Node {
                id: node.to_string(),
                web: format!("http://{}:{}", s.address.unwrap(), s.port.unwrap()),
                rpc: format!("http://{}", s.meta.unwrap().get("rpc").unwrap()),
                active: true,
            })
            .await?;

        Ok(())
    }

    async fn handle_chunks(&mut self) -> anyhow::Result<()> {
        todo!()
    }
}

fn check_if_worker_is_health(
    name: &str,
    res: Result<ApiResponse<Vec<AgentServiceChecksInfo>>, ClientError>,
) -> bool {
    match res {
        Ok(_) => {
            tracing::info!(worker_name = name, "worker node is healthy");
            true
        }
        Err(err) => match err {
            ClientError::APIError { code: 503, .. } => {
                tracing::warn!(worker_name = name, "worker node is not healthy");
                false
            }
            ClientError::APIError { .. } => {
                tracing::error!(
                    worker_name = name,
                    err = ?err,
                    "could not check service health"
                );
                false
            }
            _ => {
                tracing::error!(
                    worker_name = name,
                    err = ?err,
                    "could not check service health"
                );
                false
            }
        },
    }
}

async fn run_master_actor(mut a: MasterActor, cancellation_token: CancellationToken) {
    loop {
        tokio::select! {
            _ = cancellation_token.cancelled() => {return}
            msg = a.receiver.recv() => {
                match msg {
                    Some(msg) => if let Err(err) = a.handle(msg).await {
                        tracing::error!(err=format!("{}", err), "health check failed");
                    },
                    None => {
                        tracing::info!("master actor channel is closed, terminating");
                        return
                    }
                }

            }
        }
    }
}
