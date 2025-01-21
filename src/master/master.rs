use crate::master::consul::{Consul, Wrap};
use crate::master::node_client::{HTTPNodeClient, NodeClient};
use crate::metadata::models::{
    Chunk, ChunkLocation, ChunkUpdate, ChunkWithWeb, ChunkWithWebQueryBuilder, ChunksQueryBuilder,
    File, Node, NodeUpdate,
};
use crate::metadata::sql::MetadataStorage;
use crate::worker::router::{Order, ReplicationOrder};
use anyhow::Context;
use consulrs::api::service::common::{AgentService, AgentServiceChecksInfo};
use consulrs::api::ApiResponse;
use consulrs::client::ConsulClient;
use consulrs::error::ClientError;
use std::collections::{HashMap, HashSet};
use std::time::Duration;
use tokio::sync::{broadcast, mpsc};
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

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
        node_client: HTTPNodeClient,
        consul_client: ConsulClient,
        cancellation_token: CancellationToken,
    ) {
        let (sender, receiver) = mpsc::channel(8);
        let actor = MasterActor::new(
            receiver,
            self.unhealthy_broadcast.clone(),
            Box::new(node_client),
            self.metadata.clone(),
            Box::new(Wrap::new(consul_client)),
            self.unhealthy_threshold,
        );
        tokio::spawn(run_master_actor(actor, cancellation_token.clone()));

        let mut health_interval = tokio::time::interval(health_interval);
        let mut chunks_check_interval = tokio::time::interval(Duration::from_secs(5)); // TODO: set from main
        let mut delete_marked_chunks_interval = tokio::time::interval(Duration::from_secs(5)); // TODO: set from main
        let mut delete_marked_nodes_interval = tokio::time::interval(Duration::from_secs(5));

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
                    _ = delete_marked_chunks_interval.tick() => {
                         if let Err(err) = sender.send(MasterActorMessage::DeleteMarkedChunks).await {
                            tracing::error!(err=format!("{}", err), "could not send message");
                         }
                    }
                    _ = delete_marked_nodes_interval.tick() => {
                         if let Err(err) = sender.send(MasterActorMessage::DeleteMarkedNodes).await {
                            tracing::error!(err=format!("{}", err), "could not send message");
                         }
                    }
                    _ = cancellation_token.cancelled() => {
                        tracing::debug!("closing master");
                        return;
                    }
                }
            }
        });
    }
}

struct MasterActor {
    receiver: mpsc::Receiver<MasterActorMessage>,

    node_client: Box<dyn NodeClient + Send + Sync>,

    consul_client: Box<dyn Consul + Send + Sync>,
    metadata: MetadataStorage,

    unhealthy_broadcast: broadcast::Sender<String>, // sends info about unhealthy worker node
    unhealthy_threshold: tokio::time::Duration,
    unhealthy_nodes_duration: HashMap<String, tokio::time::Instant>,

    /// Set of all running nodes. It's cached value from the database, tracked in order no to
    /// update database each list call to consul.
    running_nodes: HashSet<String>,
}

enum MasterActorMessage {
    Health, // check workers health status form consul
    Chunks, // check if chunks' replication factor is met

    // Query chunks that are marked "to_delete" and call proper nodes to remove data.
    // If every piece was deleted set proper state in metadata db.
    DeleteMarkedChunks,

    // Query nodes that are marked as inactive, delete all related chunks from metadata
    // so these can be redistributed by Self::Chunks,
    DeleteMarkedNodes,
}

impl MasterActor {
    pub fn new(
        receiver: mpsc::Receiver<MasterActorMessage>,
        unhealthy_broadcast: broadcast::Sender<String>,
        node_client: Box<dyn NodeClient + Send + Sync>,
        metadata: MetadataStorage,
        consul_client: Box<dyn Consul + Send + Sync>,
        unhealthy_threshold: tokio::time::Duration,
    ) -> Self {
        Self {
            receiver,
            unhealthy_broadcast,
            metadata,
            consul_client,
            node_client,
            unhealthy_threshold,
            unhealthy_nodes_duration: HashMap::new(),
            running_nodes: HashSet::new(),
        }
    }

    async fn handle(&mut self, msg: MasterActorMessage) -> anyhow::Result<()> {
        match msg {
            MasterActorMessage::Health => self.handle_health().await,
            MasterActorMessage::Chunks => self.handle_chunks().await,
            MasterActorMessage::DeleteMarkedChunks => self.handle_chunks_deletion().await,
            MasterActorMessage::DeleteMarkedNodes => self.handle_nodes_deletion().await,
        }
    }

    async fn handle_health(&mut self) -> anyhow::Result<()> {
        let svcs = self.consul_client.list().await?;
        for (name, s) in svcs.response {
            let healthy = check_if_worker_is_health(&name, self.consul_client.health(&name).await);

            if healthy {
                self.reconcile_consul_db(&name, s).await?;
            } else {
                let t = self
                    .unhealthy_nodes_duration
                    .entry(name.clone())
                    .or_insert(Instant::now());

                if t.elapsed() > self.unhealthy_threshold {
                    self.running_nodes.remove(&name);
                    self.consul_client.deregister(&name).await?;
                    self.metadata
                        .update_node(
                            &name,
                            NodeUpdate {
                                active: Some(false),
                            },
                        )
                        .await?;
                    tracing::debug!(name = name, "unhealthy worker node unregistered");

                    // mark worker node as unhealthy
                    let _ = self.unhealthy_broadcast.send(name.clone());
                }
            }
        }
        Ok(())
    }

    #[tracing::instrument(skip(self))]
    async fn reconcile_consul_db(&mut self, node: &str, s: AgentService) -> anyhow::Result<()> {
        if self.running_nodes.contains(node) {
            return Ok(());
        }

        // TODO: implement logic for node with data coming back to life

        tracing::debug!(node=%node, "reconciling node");

        self.metadata
            .save_node(Node {
                id: node.to_string(),
                web: format!("http://{}:{}", s.address.unwrap(), s.port.unwrap()),
                rpc: format!("http://{}", s.meta.unwrap().get("rpc").unwrap()),
                active: true,
            })
            .await?;
        self.running_nodes.insert(node.to_string());

        Ok(())
    }

    /// It checks state of file chunks replication level. For every existing files
    /// we check every chunk_index replication level, if it's below file's replication
    /// factor, replication is ordered.
    async fn handle_chunks(&mut self) -> anyhow::Result<()> {
        tracing::debug!("starting handle chunks");
        // TODO: pagination
        let files = self
            .metadata
            .get_files()
            .await
            .context("could not get files")?;

        let nodes = self.metadata.get_nodes(Some(true)).await?;

        for file in files {
            tracing::debug!(file = ?file, "checking file replication level");
            // check a replication factor for every chunk part of a file
            let chunks = self
                .metadata
                .get_chunks(
                    ChunksQueryBuilder::default()
                        .file_id(&file.id)
                        .active_node(true)
                        .parity_shard(false)
                        .build()?,
                )
                .await?;

            let locations = self
                .metadata
                .get_chunks_with_web(
                    &ChunkWithWebQueryBuilder::default()
                        .file_name(&file.name)
                        .node_active(true)
                        .build()?,
                )
                .await
                .context("could not get chunk locations")?;

            // find where chunk_index(es) are stored (nodes)
            let chunk_locations: HashMap<i32, Vec<String>> =
                locations.iter().fold(HashMap::new(), |mut acc, loc| {
                    acc.entry(loc.chunk_index)
                        .or_insert_with(Vec::new)
                        .push(loc.web.clone());
                    acc
                });

            // file_id -> chunk_index -> count
            let mut m = HashMap::new();
            for chunk in &chunks {
                m.entry(chunk.chunk_index)
                    .and_modify(|(count, _c)| *count += 1)
                    .or_insert((1, chunk.clone()));
            }

            for (chunk_index, (replication_level, chunk)) in m {
                if replication_level < file.replication_factor {
                    if let Err(err) = self
                        .handle_chunk_under_replication(
                            chunk_index,
                            (file.replication_factor - replication_level) as usize,
                            &file,
                            &chunk,
                            &chunk_locations,
                            &nodes,
                        )
                        .await
                    {
                        tracing::error!(
                        chunk_index = chunk_index,
                        file = ?file,
                        "cannot handle chunk under replication");
                    }
                } else if replication_level > file.replication_factor {
                    if let Err(err) = self
                        .handle_chunk_over_replication(
                            chunk_index,
                            (replication_level - file.replication_factor) as usize,
                            &file,
                            &chunks,
                        )
                        .await
                    {
                        tracing::error!(
                        chunk_index = chunk_index,
                        file = ?file,
                        err = ?err,
                        "cannot handle chunk over replication");
                    }
                }
            }
        }

        Ok(())
    }

    /// Methods handles single file's chunk under replication.
    /// Firstly it finds node that contains replica of given chunk, then it finds node(s)
    /// to which we have to send this chunk.
    /// Lastly it orders found node to send data to another nodes to meet replication level.
    async fn handle_chunk_under_replication(
        &mut self,
        chunk_index: i32,
        missing: usize,
        file: &File,
        chunk: &Chunk,
        chunk_locations: &HashMap<i32, Vec<String>>,
        nodes: &Vec<Node>,
    ) -> anyhow::Result<()> {
        tracing::debug!(
                        chunk_index = chunk_index,
                        file = ?file,
                        "replication level is not met");
        // order replication of chunk_id

        // pick node that contains wanted chunk_index
        let picked_node_web = match chunk_locations.get(&chunk_index) {
            Some(v) => v,
            None => {
                tracing::error!(
                    chunk_index = chunk_index,
                    file = ?file,
                    "could not get node with given chunk index",
                );
                return Ok(());
            }
        }
        .first()
        .context("could not get node with given chunk index")?;

        // pick random node(s) that doesn't/don't, pick them that much to meet replication factor level
        let nodes_to_pick = filter_out_nodes_without_chunk(nodes, &chunk_locations, chunk_index)?;

        if nodes_to_pick.is_empty() {
            tracing::debug!(
                  chunk_index = chunk_index,
                    file = ?file,
                    "there are no nodes without given chunk, skipping",
            );
            return Ok(());
        }

        let picked = if nodes_to_pick.len() < missing {
            tracing::warn!(
                chunk_index = chunk_index,
                file = ?file,
                "too few nodes to meet replication factor level for chunk",
            );
            nodes_to_pick
        } else {
            nodes_to_pick[..missing].to_vec()
        };

        let mut orders = vec![];
        for node in picked {
            // save info about newly create chunk
            let new_chunk_id = Uuid::new_v4().to_string();

            self.metadata
                .tx(|uow| {
                    uow.save_chunk(Chunk::new(
                        new_chunk_id.clone(),
                        file.id.clone(),
                        chunk_index,
                    ))?;
                    uow.save_chunk_location(ChunkLocation {
                        chunk_id: new_chunk_id.clone(),
                        node_id: node.id,
                    })
                    .context("could not save chunk location")?;
                    Ok(())
                })
                .await?;

            orders.push(Order {
                new_chunk_id,
                node_web: node.web.clone(),
            });
        }

        // TODO: fail over to another node is this one turns out to be dead
        self.node_client
            .replicate(picked_node_web, &chunk.id, ReplicationOrder { orders })
            .await
            .context("could not order replication")?;

        Ok(())
    }

    #[tracing::instrument(skip(self))]
    async fn handle_chunk_over_replication(
        &mut self,
        chunk_index: i32,
        redundant: usize,
        file: &File,
        chunks: &[Chunk],
    ) -> anyhow::Result<()> {
        tracing::debug!(
            chunk_index = chunk_index,
            file = ?file,
            redundant = redundant,
            "replication level too high, deleting redundant replicas");

        dbg!(chunk_index);

        let picked: Vec<&Chunk> = chunks
            .iter()
            .filter(|c| c.chunk_index == chunk_index && c.file_id == file.id)
            .collect();
        let picked = &picked[..redundant.min(picked.len())] // delete only those that we need
            .to_vec();

        dbg!(&picked);

        for ch in picked {
            dbg!(&ch.id);
            self.metadata
                .update_chunk(
                    &ch.id,
                    ChunkUpdate {
                        to_delete: Some(true),
                    },
                )
                .await?; // mark for async deletion
        }

        Ok(())
    }

    /// Takes care of chunks that are marked "to_delete"
    async fn handle_chunks_deletion(&mut self) -> anyhow::Result<()> {
        let chunks_with_web = self
            .metadata
            .get_chunks_with_web(
                &ChunkWithWebQueryBuilder::default()
                    .to_delete(true)
                    .build()?,
            )
            .await
            .context("could not get chunks")?;

        let mut files_to_check = vec![];
        for ChunkWithWeb {
            chunk_id,
            web,
            file_id,
            node_active,
            ..
        } in chunks_with_web
        {
            files_to_check.push(file_id);
            if node_active {
                self.node_client
                    .delete_chunk(&chunk_id, &web)
                    .await
                    .with_context(|| {
                        format!("could not delete chunk from storage node {}", chunk_id)
                    })?;
            }

            // NOTE: in case of errors (and retries) we should eventually delete chunk from the database
            // as node returns 204 in case of non-existing file.
            self.metadata
                .delete_chunk(&chunk_id)
                .await
                .context("could not delete chunk from metadata db")?;
        }

        for file_id in files_to_check {
            let chunks = self
                .metadata
                .get_chunks(ChunksQueryBuilder::default().file_id(&file_id).build()?)
                .await?;
            if chunks.is_empty() {
                self.metadata.delete_file(&file_id).await?;
            }
        }

        Ok(())
    }

    /// Takes care of nodes that are marked "inactive".
    async fn handle_nodes_deletion(&mut self) -> anyhow::Result<()> {
        let nodes = self.metadata.get_nodes(Some(false)).await?;

        for node in nodes {
            let chunks = self
                .metadata
                .get_chunks(ChunksQueryBuilder::default().node_id(node.id).build()?)
                .await?;
            for chunk in chunks {
                // mark to deletion, will be done in separate process (handle_chunks_deletion)
                self.metadata
                    .update_chunk(
                        &chunk.id,
                        ChunkUpdate {
                            to_delete: Some(true),
                        },
                    )
                    .await?;
            }
        }

        Ok(())
    }
}

/// Returns vector of Nodes that don't contain given chunk_index;
fn filter_out_nodes_without_chunk(
    nodes: &Vec<Node>,
    chunk_locations: &HashMap<i32, Vec<String>>,
    chunk_index: i32,
) -> anyhow::Result<Vec<Node>> {
    let mut res = vec![];
    let nodes_with_chunk = chunk_locations
        .get(&chunk_index)
        .context("could not get chunk's nodes")?;

    for node in nodes {
        if !nodes_with_chunk.contains(&node.web) {
            res.push(node.clone());
        }
    }
    Ok(res)
}

fn check_if_worker_is_health(
    name: &str,
    res: Result<ApiResponse<Vec<AgentServiceChecksInfo>>, ClientError>,
) -> bool {
    match res {
        Ok(_) => {
            tracing::debug!(worker_name = name, "worker node is healthy");
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
                        tracing::error!(err=?err, "health check failed");
                    },
                    None => {
                        tracing::debug!("master actor channel is closed, terminating");
                        return
                    }
                }

            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::master::consul::MockConsul;
    use crate::master::node_client::MockNodeClient;
    use crate::metadata::models::{ChunksQuery, File};
    use chrono::NaiveDateTime;
    use diesel::{Connection, SqliteConnection};
    use diesel_migrations::{embed_migrations, EmbeddedMigrations, MigrationHarness};
    use rand::distr::Alphanumeric;
    use rand::Rng;
    use std::env::temp_dir;
    use std::sync::Arc;
    use tokio::sync::Mutex;

    pub const MIGRATIONS: EmbeddedMigrations = embed_migrations!("migrations");

    #[tokio::test]
    async fn test_handle_chunks() -> anyhow::Result<()> {
        let (mut storage, receiver, s) = setup().await?;

        let mock_consul = MockConsul::new();
        let mut mock_node_client = MockNodeClient::new();
        mock_node_client
            .expect_replicate()
            .withf(|picked_node_web, chunk_id, rorder| {
                picked_node_web == "web1"
                    && chunk_id == "chunk1"
                    && rorder.orders[0].node_web == "web2".to_string()
            })
            .returning(|_, _, _| Ok(()));
        mock_node_client
            .expect_replicate()
            .withf(|picked_node_web, chunk_id, rorder| {
                picked_node_web == "web1"
                    && chunk_id == "chunk2"
                    && rorder.orders[0].node_web == "web2".to_string()
            })
            .returning(|_, _, _| Ok(()));

        let mut actor = MasterActor::new(
            receiver,
            s,
            Box::new(mock_node_client),
            storage.clone(),
            Box::new(mock_consul),
            Duration::from_millis(100),
        );

        // prep state
        let fid = Uuid::new_v4().to_string();
        let nid = Uuid::new_v4().to_string();
        let nid2 = Uuid::new_v4().to_string();
        prepare_state(&mut storage, fid.clone(), 3, nid.clone(), nid2.clone()).await?;

        // logic
        actor.handle_chunks().await?;

        // assert changes
        {
            let chunks = storage.get_chunks(ChunksQuery::default()).await?;
            assert_eq!(chunks.len(), 4);
            let mapped = chunks.into_iter().fold(HashMap::new(), |mut acc, chunk| {
                acc.entry(chunk.chunk_index)
                    .and_modify(|v| *v += 1)
                    .or_insert(1);
                acc
            });
            assert_eq!(mapped.get(&0), Some(&2));
            assert_eq!(mapped.get(&1), Some(&2));
        }
        {
            let locations = storage
                .get_chunks_with_web(
                    &ChunkWithWebQueryBuilder::default()
                        .file_name("test-file")
                        .node_active(true)
                        .build()?,
                )
                .await?;
            assert_eq!(locations.len(), 4);
            let mapped = locations
                .into_iter()
                .fold(HashMap::new(), |mut acc, chunk| {
                    acc.entry(chunk.web).and_modify(|v| *v += 1).or_insert(1);
                    acc
                });
            assert_eq!(mapped.get("web1"), Some(&2));
            assert_eq!(mapped.get("web2"), Some(&2));
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_handle_chunks_over_replication() -> anyhow::Result<()> {
        let (mut storage, receiver, s) = setup().await?;

        let mock_consul = MockConsul::new();
        let mut mock_node_client = MockNodeClient::new();
        mock_node_client
            .expect_delete_chunk()
            .withf(|name, node_url| {
                if name == "chunk1" {
                    return node_url == "web1";
                } else if name == "chunk2" {
                    return node_url == "web1";
                } else if name == "chunk3" {
                    return node_url == "web2";
                }
                false
            })
            .returning(|_, _| Ok(()));

        let mut actor = MasterActor::new(
            receiver,
            s,
            Box::new(mock_node_client),
            storage.clone(),
            Box::new(mock_consul),
            Duration::from_millis(100),
        );

        // prep state
        let fid = Uuid::new_v4().to_string();
        let nid = Uuid::new_v4().to_string();
        let nid2 = Uuid::new_v4().to_string();
        prepare_over_replication_state(&mut storage, fid.clone(), nid.clone(), nid2.clone())
            .await?;

        // logic
        actor.handle_chunks().await?;

        // assert changes
        {
            let chunks = storage
                .get_chunks(
                    ChunksQueryBuilder::default()
                        .to_delete(false)
                        .build()
                        .unwrap(),
                )
                .await?;
            assert_eq!(chunks.len(), 1);
        }
        {
            let locations = storage
                .get_chunks_with_web(
                    &ChunkWithWebQueryBuilder::default()
                        .file_name("test-file")
                        .node_active(true)
                        .to_delete(false)
                        .build()?,
                )
                .await?;
            assert_eq!(locations.len(), 1);
        }

        Ok(())
    }

    async fn setup() -> anyhow::Result<(
        MetadataStorage,
        mpsc::Receiver<MasterActorMessage>,
        broadcast::Sender<String>,
    )> {
        let test_dir = temp_dir();
        let s: String = rand::thread_rng()
            .sample_iter(&Alphanumeric)
            .take(7)
            .map(char::from)
            .collect();
        let db_path = test_dir.join(format!("test_sql_{s}.db"));
        tokio::fs::File::create(&db_path).await?;

        let conn = Arc::new(Mutex::new(SqliteConnection::establish(
            &db_path.to_str().unwrap(),
        )?));

        {
            let mut guard = conn.lock().await;
            guard.run_pending_migrations(MIGRATIONS).unwrap();
        }

        let (_, receiver) = mpsc::channel(8);
        let (s, _) = broadcast::channel(8);

        Ok((MetadataStorage::new(conn.clone()), receiver, s))
    }

    async fn prepare_state(
        storage: &mut MetadataStorage,
        fid: String,
        replication_factor: i32,
        nid: String,
        nid2: String,
    ) -> anyhow::Result<()> {
        storage
            .save_file(&File {
                id: fid.clone(),
                replication_factor,
                created_at: NaiveDateTime::default(),
                modified_at: NaiveDateTime::default(),
                name: "test-file".to_string(),
                to_delete: false,
            })
            .await?;
        storage
            .save_node(Node {
                id: nid.clone(),
                web: "web1".to_string(),
                active: true,
                rpc: "web1".to_string(),
            })
            .await?;
        storage
            .save_node(Node {
                id: nid2,
                web: "web2".to_string(),
                active: true,
                rpc: "web2".to_string(),
            })
            .await?;

        storage
            .save_chunks(vec![
                Chunk::new("chunk1".to_string(), fid.clone(), 0),
                Chunk::new("chunk2".to_string(), fid.clone(), 1),
            ])
            .await?;

        storage
            .save_chunk_locations(vec![
                ChunkLocation {
                    chunk_id: "chunk1".to_string(),
                    node_id: nid.clone(),
                },
                ChunkLocation {
                    chunk_id: "chunk2".to_string(),
                    node_id: nid.clone(),
                },
            ])
            .await?;

        Ok(())
    }

    async fn prepare_over_replication_state(
        storage: &mut MetadataStorage,
        fid: String,
        nid: String,
        nid2: String,
    ) -> anyhow::Result<()> {
        storage
            .save_file(&File {
                id: fid.clone(),
                replication_factor: 1,
                created_at: NaiveDateTime::default(),
                modified_at: NaiveDateTime::default(),
                name: "test-file".to_string(),
                to_delete: false,
            })
            .await?;
        storage
            .save_node(Node {
                id: nid.clone(),
                web: "web1".to_string(),
                active: true,
                rpc: "web1".to_string(),
            })
            .await?;
        storage
            .save_node(Node {
                id: nid2.clone(),
                web: "web2".to_string(),
                active: true,
                rpc: "web2".to_string(),
            })
            .await?;

        storage
            .save_chunks(vec![
                Chunk::new("chunk1".to_string(), fid.clone(), 0),
                Chunk::new("chunk2".to_string(), fid.clone(), 0),
                Chunk::new("chunk3".to_string(), fid.clone(), 0),
            ])
            .await?;

        storage
            .save_chunk_locations(vec![
                ChunkLocation {
                    chunk_id: "chunk1".to_string(),
                    node_id: nid.clone(),
                },
                ChunkLocation {
                    chunk_id: "chunk2".to_string(),
                    node_id: nid.clone(),
                },
                ChunkLocation {
                    chunk_id: "chunk3".to_string(),
                    node_id: nid2.clone(),
                },
            ])
            .await?;

        Ok(())
    }
}
