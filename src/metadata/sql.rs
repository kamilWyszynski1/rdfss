use crate::metadata::models::{Chunk, Node, NodeUpdate};
use crate::schema::*;
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
        Self { conn }
    }

    async fn run_in_conn<F, T>(&self, f: F) -> anyhow::Result<T>
    where
        F: Fn(&mut SqliteConnection) -> anyhow::Result<T>,
    {
        let mut conn = self.conn.lock().await;
        f(&mut conn)
    }

    #[tracing::instrument]
    pub async fn save_node(&mut self, node: Node) -> anyhow::Result<()> {
        self.run_in_conn(|conn| {
            diesel::insert_into(nodes::table)
                .values(&node)
                .execute(&mut *conn)?;
            Ok(())
        })
        .await?;
        Ok(())
    }

    #[tracing::instrument]
    pub async fn get_node(&mut self, nid: &str) -> anyhow::Result<Option<Node>> {
        self.run_in_conn(|conn| {
            let n = nodes::table
                .find(nid)
                .select(Node::as_select())
                .first(conn)
                .optional()?;
            Ok(n)
        })
        .await
    }

    /// Returns all active nodes.
    #[tracing::instrument]
    pub async fn get_nodes(&mut self, active: Option<bool>) -> anyhow::Result<Vec<Node>> {
        self.run_in_conn(move |conn| {
            let mut query = nodes::table.into_boxed();
            if let Some(a) = active {
                query = query.filter(nodes::active.eq(a))
            }

            let n = query.load::<Node>(conn)?;
            Ok(n)
        })
        .await
    }

    pub async fn update_node(&mut self, id: &str, node: NodeUpdate) -> anyhow::Result<()> {
        self.run_in_conn(|conn| {
            diesel::update(nodes::table.find(id))
                .set(&node)
                .execute(conn)?;
            Ok(())
        })
        .await?;
        Ok(())
    }

    #[tracing::instrument]
    pub async fn get_chunks(&mut self, fname: &str) -> anyhow::Result<Vec<(String, String)>> {
        self.run_in_conn(|conn| {
            let a = chunks::table
                .inner_join(nodes::table)
                .filter(chunks::filename.eq(fname))
                .select((chunks::id, nodes::web))
                .load(conn)?;
            Ok(a)
        })
        .await
    }

    #[tracing::instrument]
    pub async fn file_exists(&mut self, fname: &str) -> anyhow::Result<bool> {
        let v = self
            .run_in_conn(|conn| {
                Ok(diesel::select(diesel::dsl::exists(
                    chunks::table.filter(chunks::filename.eq(fname)),
                ))
                .get_result(conn)?)
            })
            .await?;
        tracing::debug!(v = v, "exists");
        Ok(v)
    }

    #[tracing::instrument]
    pub async fn save_chunk(&mut self, chunk: Chunk) -> anyhow::Result<()> {
        self.run_in_conn(|conn| {
            diesel::insert_into(chunks::table)
                .values(&chunk)
                .execute(conn)?;
            Ok(())
        })
        .await
    }

    #[tracing::instrument]
    pub async fn delete_chunk(&mut self, id: &str) -> anyhow::Result<()> {
        self.run_in_conn(|conn| {
            diesel::delete(chunks::table.filter(chunks::id.eq(id))).execute(conn)?;
            Ok(())
        })
        .await
    }

    #[tracing::instrument]
    pub async fn save_chunks(&mut self, chunks: Vec<Chunk>) -> anyhow::Result<()> {
        tracing::info!("saving chunks");
        self.run_in_conn(|conn| {
            diesel::insert_into(chunks::table)
                .values(&chunks)
                .execute(conn)?;
            Ok(())
        })
        .await
    }
}
