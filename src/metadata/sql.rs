use crate::metadata::models::{
    Chunk, ChunkLocation, ChunkUpdate, ChunkWithWeb, ChunkWithWebQuery, ChunksQuery, File,
    FileUpdate, FilesQuery, Node, NodeUpdate,
};
use crate::schema::*;
use diesel::associations::HasTable;
use diesel::prelude::*;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;
use tokio::sync::Mutex;

#[derive(Clone)]
pub struct MetadataStorage {
    conn: Arc<Mutex<SqliteConnection>>,
}

impl Debug for MetadataStorage {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "MetadataStorage")
    }
}

impl MetadataStorage {
    pub fn new(conn: Arc<Mutex<SqliteConnection>>) -> Self {
        Self { conn: conn.clone() }
    }

    async fn run_in_conn<F, T>(&self, f: F) -> anyhow::Result<T>
    where
        F: FnOnce(&mut SqliteConnection) -> anyhow::Result<T>,
    {
        let mut conn = self.conn.lock().await;
        f(&mut conn)
    }

    #[tracing::instrument]
    pub async fn save_file(&mut self, file: &File) -> anyhow::Result<()> {
        self.run_in_conn(|conn| UOW::new(conn).save_file(file))
            .await?;
        Ok(())
    }

    #[tracing::instrument]
    pub async fn get_file(&mut self, query: &FilesQuery) -> anyhow::Result<File> {
        Ok(self
            .run_in_conn(|conn| UOW::new(conn).get_file(query))
            .await?)
    }

    /// Returns all active nodes.
    #[tracing::instrument]
    pub async fn get_files(&mut self) -> anyhow::Result<Vec<File>> {
        self.run_in_conn(move |conn| UOW::new(conn).get_files())
            .await
    }

    #[tracing::instrument]
    pub async fn delete_file(&mut self, id: &str) -> anyhow::Result<()> {
        self.run_in_conn(|conn| UOW::new(conn).delete_file(id))
            .await
    }

    #[tracing::instrument]
    pub async fn file_exists(&mut self, fname: &str) -> anyhow::Result<bool> {
        let v = self
            .run_in_conn(|conn| Ok(UOW::new(conn).file_exists(fname)?))
            .await?;
        tracing::debug!(v = v, "exists");
        Ok(v)
    }

    #[tracing::instrument]
    pub async fn delete_file_by_name(&mut self, name: &str) -> anyhow::Result<()> {
        self.run_in_conn(|conn| {
            UOW::new(conn).delete_file_by_name(name)?;
            Ok(())
        })
        .await
    }

    #[tracing::instrument]
    pub async fn save_node(&mut self, node: Node) -> anyhow::Result<()> {
        self.run_in_conn(|conn| {
            UOW::new(conn).save_node(node)?;
            Ok(())
        })
        .await?;
        Ok(())
    }

    #[tracing::instrument]
    pub async fn get_node(&mut self, nid: &str) -> anyhow::Result<Option<Node>> {
        self.run_in_conn(|conn| Ok(UOW::new(conn).get_node(nid)?))
            .await
    }

    /// Returns all active nodes.
    #[tracing::instrument]
    pub async fn get_nodes(&mut self, active: Option<bool>) -> anyhow::Result<Vec<Node>> {
        self.run_in_conn(move |conn| Ok(UOW::new(conn).get_nodes(active)?))
            .await
    }

    pub async fn update_node(&mut self, id: &str, node: NodeUpdate) -> anyhow::Result<()> {
        self.run_in_conn(|conn| {
            UOW::new(conn).update_node(id, node)?;
            Ok(())
        })
        .await?;
        Ok(())
    }

    #[tracing::instrument]
    pub async fn get_chunks(&mut self, chunks_query: ChunksQuery) -> anyhow::Result<Vec<Chunk>> {
        self.run_in_conn(move |conn| UOW::new(conn).get_chunks(chunks_query))
            .await
    }

    #[tracing::instrument]
    pub async fn get_chunks_with_web(
        &mut self,
        query: &ChunkWithWebQuery,
    ) -> anyhow::Result<Vec<ChunkWithWeb>> {
        self.run_in_conn(|conn| Ok(UOW::new(conn).get_chunks_with_web(query)?))
            .await
    }

    #[tracing::instrument]
    pub async fn get_chunk_id_by_index_and_node(
        &mut self,
        chunk_index: i32,
        node_web: &str,
    ) -> anyhow::Result<String> {
        self.run_in_conn(|conn| {
            Ok(UOW::new(conn).get_chunk_id_by_index_and_node(chunk_index, node_web)?)
        })
        .await
    }

    #[tracing::instrument]
    pub async fn save_chunk(&mut self, chunk: Chunk) -> anyhow::Result<()> {
        self.run_in_conn(|conn| {
            UOW::new(conn).save_chunk(chunk)?;
            Ok(())
        })
        .await
    }

    #[tracing::instrument]
    pub async fn update_chunk(&mut self, id: &str, update: ChunkUpdate) -> anyhow::Result<()> {
        self.run_in_conn(|conn| {
            UOW::new(conn).update_chunk(id, update)?;
            Ok(())
        })
        .await
    }

    /// Removes chunk from chunks and chunk_locations in single transaction.
    #[tracing::instrument]
    pub async fn delete_chunk(&mut self, id: &str) -> anyhow::Result<()> {
        self.run_in_conn(|conn| {
            UOW::new(conn).delete_chunk(id)?;
            UOW::new(conn).delete_chunk_from_chunk_locations(id)?;
            Ok(())
        })
        .await
    }

    #[tracing::instrument]
    pub async fn save_chunks(&mut self, chunks: Vec<Chunk>) -> anyhow::Result<()> {
        tracing::info!("saving chunks");
        self.run_in_conn(|conn| {
            UOW::new(conn).save_chunks(chunks)?;
            Ok(())
        })
        .await
    }

    #[tracing::instrument]
    pub async fn save_chunk_location(
        &mut self,
        chunk_location: ChunkLocation,
    ) -> anyhow::Result<()> {
        tracing::info!("saving chunk locations");
        self.run_in_conn(|conn| {
            UOW::new(conn).save_chunk_location(chunk_location)?;
            Ok(())
        })
        .await
    }

    #[tracing::instrument]
    pub async fn save_chunk_locations(
        &mut self,
        chunk_locations: Vec<ChunkLocation>,
    ) -> anyhow::Result<()> {
        tracing::info!("saving chunk locations");
        self.run_in_conn(|conn| {
            UOW::new(conn).save_chunk_locations(chunk_locations)?;
            Ok(())
        })
        .await
    }

    pub async fn tx<F>(&mut self, f: F) -> anyhow::Result<()>
    where
        F: FnOnce(&mut UOW) -> anyhow::Result<()>,
    {
        self.run_in_conn(|conn| {
            conn.transaction::<(), _, _>(|conn| {
                let mut uow = UOW::new(conn);
                f(&mut uow)
            })
        })
        .await
    }
}

pub struct UOW<'a> {
    conn: &'a mut SqliteConnection,
}

impl<'a> UOW<'a> {
    pub fn new(conn: &'a mut SqliteConnection) -> Self {
        Self { conn }
    }

    pub fn save_file(&mut self, file: &File) -> anyhow::Result<()> {
        diesel::insert_into(files::table)
            .values(file)
            .execute(self.conn)?;
        Ok(())
    }

    pub fn get_file(&mut self, fq: &FilesQuery) -> anyhow::Result<File> {
        let mut query = files::table.into_boxed();
        if let Some(file_name) = &fq.name {
            query = query.filter(files::name.eq(file_name))
        }
        if let Some(to_delete) = &fq.to_delete {
            query = query.filter(files::to_delete.eq(to_delete))
        }

        Ok(query.get_result(self.conn)?)
    }

    pub fn get_files(&mut self) -> anyhow::Result<Vec<File>> {
        Ok(files::table.load(self.conn)?)
    }

    pub fn update_file(&mut self, id: &str, update: &FileUpdate) -> anyhow::Result<()> {
        diesel::update(files::table.find(id))
            .set(update)
            .execute(self.conn)?;
        Ok(())
    }

    pub fn delete_file(&mut self, id: &str) -> anyhow::Result<()> {
        diesel::delete(files::table.filter(files::id.eq(id))).execute(self.conn)?;
        Ok(())
    }

    pub fn delete_file_by_name(&mut self, name: &str) -> anyhow::Result<()> {
        diesel::delete(files::table.filter(files::name.eq(name))).execute(self.conn)?;
        Ok(())
    }

    pub fn file_exists(&mut self, fname: &str) -> anyhow::Result<bool> {
        Ok(diesel::select(diesel::dsl::exists(
            files::table.filter(files::name.eq(fname)),
        ))
        .get_result(self.conn)?)
    }

    pub fn save_node(&mut self, node: Node) -> anyhow::Result<()> {
        diesel::insert_into(nodes::table)
            .values(&node)
            .on_conflict(nodes::id)
            .do_update()
            .set(nodes::active.eq(node.active))
            .execute(self.conn)?;
        Ok(())
    }

    pub fn get_node(&mut self, nid: &str) -> anyhow::Result<Option<Node>> {
        let n = nodes::table
            .find(nid)
            .select(Node::as_select())
            .first(self.conn)
            .optional()?;
        Ok(n)
    }

    pub fn get_nodes(&mut self, active: Option<bool>) -> anyhow::Result<Vec<Node>> {
        let mut query = nodes::table.into_boxed();
        if let Some(a) = active {
            query = query.filter(nodes::active.eq(a))
        }

        let n = query.load::<Node>(self.conn)?;
        Ok(n)
    }

    pub fn update_node(&mut self, id: &str, node: NodeUpdate) -> anyhow::Result<()> {
        diesel::update(nodes::table.find(id))
            .set(&node)
            .execute(self.conn)?;
        Ok(())
    }

    pub fn get_chunks(&mut self, chunks_query: ChunksQuery) -> anyhow::Result<Vec<Chunk>> {
        if let Some(active) = &chunks_query.active_node {
            let mut joined = chunk_locations::table
                .inner_join(chunks::table)
                .inner_join(nodes::table)
                .select(chunks::all_columns)
                .filter(nodes::active.eq(active))
                .into_boxed();

            if let Some(id) = &chunks_query.file_id {
                joined = joined.filter(chunks::file_id.eq(id))
            }
            if let Some(to_delete) = &chunks_query.to_delete {
                joined = joined.filter(chunks::to_delete.eq(to_delete))
            }
            if let Some(node_id) = &chunks_query.node_id {
                joined = joined.filter(nodes::id.eq(node_id))
            }
            if let Some(parity_shard) = &chunks_query.parity_shard {
                joined = joined.filter(chunks::parity_shard.eq(parity_shard))
            }
            return Ok(joined.load(self.conn)?);
        } else if let Some(node_id) = &chunks_query.node_id {
            let mut joined = chunk_locations::table
                .inner_join(chunks::table)
                .inner_join(nodes::table)
                .select(chunks::all_columns)
                .filter(nodes::id.eq(node_id))
                .into_boxed();

            if let Some(id) = &chunks_query.file_id {
                joined = joined.filter(chunks::file_id.eq(id))
            }
            if let Some(to_delete) = &chunks_query.to_delete {
                joined = joined.filter(chunks::to_delete.eq(to_delete))
            }
            if let Some(parity_shard) = &chunks_query.parity_shard {
                joined = joined.filter(chunks::parity_shard.eq(parity_shard))
            }
            return Ok(joined.load(self.conn)?);
        }

        let mut query = chunks::table.into_boxed();
        if let Some(id) = &chunks_query.file_id {
            query = query.filter(chunks::file_id.eq(id))
        }
        if let Some(to_delete) = &chunks_query.to_delete {
            query = query.filter(chunks::to_delete.eq(to_delete))
        }

        Ok(query.load(self.conn)?)
    }

    pub fn get_chunks_with_web(
        &mut self,
        query: &ChunkWithWebQuery,
    ) -> anyhow::Result<Vec<ChunkWithWeb>> {
        let mut boxed = chunk_locations::table
            .inner_join(nodes::table)
            .inner_join(chunks::table)
            .inner_join(files::table.on(chunks::file_id.eq(files::id)))
            .select((
                chunk_locations::chunk_id,
                chunks::chunk_index,
                nodes::web,
                files::id,
                nodes::active,
                chunks::parity_shard,
            ))
            .into_boxed();

        if let Some(fname) = &query.file_name {
            boxed = boxed.filter(files::name.eq(fname));
        }
        if let Some(to_delete) = &query.to_delete {
            boxed = boxed.filter(chunks::to_delete.eq(to_delete));
        }
        if let Some(node_active) = &query.node_active {
            boxed = boxed.filter(nodes::active.eq(node_active));
        }
        if let Some(parity_shard) = &query.parity_shard {
            boxed = boxed.filter(chunks::parity_shard.eq(parity_shard));
        }

        Ok(boxed.load(self.conn)?)
    }

    pub fn get_chunk_id_by_index_and_node(
        &mut self,
        chunk_index: i32,
        node_web: &str,
    ) -> anyhow::Result<String> {
        Ok(chunks::table
            .inner_join(chunk_locations::table.inner_join(nodes::table))
            .filter(nodes::web.eq(node_web))
            .filter(chunks::chunk_index.eq(chunk_index))
            .select(chunks::id)
            .get_result(self.conn)?)
    }

    pub fn save_chunk(&mut self, chunk: Chunk) -> anyhow::Result<()> {
        diesel::insert_into(chunks::table)
            .values(&chunk)
            .execute(self.conn)?;
        Ok(())
    }

    pub fn update_chunk(&mut self, id: &str, update: ChunkUpdate) -> anyhow::Result<()> {
        diesel::update(chunks::table.find(id))
            .set(&update)
            .execute(self.conn)?;
        Ok(())
    }

    fn delete_chunk(&mut self, id: &str) -> anyhow::Result<()> {
        diesel::delete(chunks::table.filter(chunks::id.eq(id))).execute(self.conn)?;
        Ok(())
    }

    fn delete_chunk_from_chunk_locations(&mut self, id: &str) -> anyhow::Result<()> {
        diesel::delete(chunk_locations::table.filter(chunk_locations::chunk_id.eq(id)))
            .execute(self.conn)?;
        Ok(())
    }

    pub fn save_chunks(&mut self, chunks: Vec<Chunk>) -> anyhow::Result<()> {
        diesel::insert_into(chunks::table)
            .values(&chunks)
            .execute(self.conn)?;
        Ok(())
    }

    pub fn save_chunk_location(&mut self, chunk_location: ChunkLocation) -> anyhow::Result<()> {
        tracing::info!("saving chunk locations");
        diesel::insert_into(chunk_locations::table)
            .values(&chunk_location)
            .execute(self.conn)?;
        Ok(())
    }

    pub fn save_chunk_locations(
        &mut self,
        chunk_locations: Vec<ChunkLocation>,
    ) -> anyhow::Result<()> {
        diesel::insert_into(chunk_locations::table)
            .values(&chunk_locations)
            .execute(self.conn)?;
        Ok(())
    }
}
