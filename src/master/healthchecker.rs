use crate::health;
use crate::health::healthchecker_client::HealthcheckerClient;
use crate::metadata::models::NodeUpdate;
use crate::metadata::sql::MetadataStorage;
use std::collections::HashMap;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tonic::transport::Channel;

#[derive(Debug, Clone)]
pub struct Client {
    client: health::healthchecker_client::HealthcheckerClient<Channel>,
    node_id: String,
}

impl
    From<(
        String,
        health::healthchecker_client::HealthcheckerClient<Channel>,
    )> for Client
{
    fn from(value: (String, HealthcheckerClient<Channel>)) -> Self {
        Self {
            client: value.1,
            node_id: value.0,
        }
    }
}

pub struct MasterHealthChecker {
    clients: Vec<Client>,
    metadata: MetadataStorage,
    inactive_threshold: u8,
}

impl MasterHealthChecker {
    pub fn new(
        clients: Vec<(
            String,
            health::healthchecker_client::HealthcheckerClient<Channel>,
        )>,
        metadata: MetadataStorage,
    ) -> Self {
        Self {
            clients: clients.into_iter().map(|v| v.into()).collect(),
            metadata,
            inactive_threshold: 5,
        }
    }

    pub fn with_inactive_threshold(mut self, inactive_threshold: u8) -> Self {
        self.inactive_threshold = inactive_threshold;
        self
    }

    pub async fn run(&self, interval: Duration, cancellation_token: CancellationToken) {
        let (sender, receiver) = mpsc::channel(8);
        let actor = HealthcheckActor::new(
            receiver,
            self.clients.clone(),
            self.metadata.clone(),
            self.inactive_threshold,
        );
        tokio::spawn(run_healthcheck_actor(actor, cancellation_token.clone()));

        let mut interval = tokio::time::interval(interval);

        tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = interval.tick() => {
                        if let Err(err) = sender.send(()).await {
                            tracing::error!(err=format!("{}", err), "could not send message");
                        }
                    }
                    _ = cancellation_token.cancelled() => {
                        tracing::info!("closing master healthchecker");
                        return;
                    }
                }
            }
        });
    }
}

struct HealthcheckActor {
    receiver: mpsc::Receiver<()>,
    clients: Vec<(Client, bool)>,

    metadata: MetadataStorage,
    inactive_threshold: u8,

    failed: HashMap<usize, u8>,
}

impl HealthcheckActor {
    fn new(
        receiver: mpsc::Receiver<()>,
        clients: Vec<Client>,
        metadata: MetadataStorage,
        inactive_threshold: u8,
    ) -> Self {
        Self {
            receiver,
            clients: clients.into_iter().map(|client| (client, false)).collect(),
            metadata,
            inactive_threshold,
            failed: HashMap::new(),
        }
    }

    async fn handle(&mut self) -> anyhow::Result<()> {
        for (inx, (client, active)) in &mut self.clients.iter_mut().enumerate() {
            if !*active {
                continue;
            }
            if let Err(err) = client.client.health(()).await {
                tracing::error!(err = format!("{}", err), "health check failed");

                let v = self.failed.entry(inx).or_insert(0);
                *v += 1;
                if *v > self.inactive_threshold {
                    self.metadata
                        .update_node(
                            &client.node_id,
                            NodeUpdate {
                                active: Some(false),
                            },
                        )
                        .await?;
                    *active = false;
                }
            }
        }

        Ok(())
    }
}

async fn run_healthcheck_actor(mut a: HealthcheckActor, cancellation_token: CancellationToken) {
    loop {
        tokio::select! {
            _ = cancellation_token.cancelled() => {return}
            _ = a.receiver.recv() => {
                if let Err(err) = a.handle().await {
                    tracing::error!(err=format!("{}", err), "health check failed");
                }
            }
        }
    }
}
