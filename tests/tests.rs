extern crate diesel_migrations;
use anyhow;
use diesel::{Connection, SqliteConnection};
use diesel_migrations::{embed_migrations, EmbeddedMigrations, MigrationHarness};
use rdfss::metadata::models::{Chunk, Node, NodeUpdate};
use rdfss::metadata::sql::MetadataStorage;
use std::env::temp_dir;
use std::sync::Arc;
use tokio::sync::Mutex;

pub const MIGRATIONS: EmbeddedMigrations = embed_migrations!("migrations");

#[tokio::test]
async fn test_sql() -> anyhow::Result<()> {
    let test_dir = temp_dir();
    let db_path = test_dir.join("test_sql.db");
    tokio::fs::File::create(&db_path).await?;

    let conn = Arc::new(Mutex::new(SqliteConnection::establish(
        &db_path.to_str().unwrap(),
    )?));
    let mut storage = MetadataStorage::new(conn.clone());

    {
        let mut guard = conn.lock().await;
        guard.run_pending_migrations(MIGRATIONS).unwrap();
    }

    storage
        .save_node(Node {
            id: "id1".to_string(),
            web: "localhost:3001".to_string(),
            rpc: "localhost:50051".to_string(),
            active: true,
        })
        .await?;
    storage
        .save_node(Node {
            id: "id2".to_string(),
            web: "localhost:3002".to_string(),
            rpc: "localhost:50052".to_string(),
            active: true,
        })
        .await?;

    let nodes = storage.get_nodes(None).await?;
    assert_eq!(nodes.len(), 2);

    storage
        .save_chunks(vec![
            Chunk {
                id: "chunk1".to_string(),
                filename: "file1".to_string(),
                node_id: "id1".to_string(),
            },
            Chunk {
                id: "chunk2".to_string(),
                filename: "file1".to_string(),
                node_id: "id2".to_string(),
            },
        ])
        .await?;

    let chunks = storage.get_chunks("file1").await?;
    assert_eq!(
        chunks,
        vec![
            ("chunk1".to_string(), "localhost:3001".to_string()),
            ("chunk2".to_string(), "localhost:3002".to_string())
        ]
    );

    storage.delete_chunk("chunk1").await?;
    let chunks = storage.get_chunks("file1").await?;
    assert_eq!(
        chunks,
        vec![("chunk2".to_string(), "localhost:3002".to_string()),]
    );

    assert!(storage.file_exists("file1").await?);

    storage.delete_chunk("chunk2").await?;

    assert_eq!(storage.file_exists("file1").await?, false);

    storage
        .update_node(
            "id2",
            NodeUpdate {
                active: Some(false),
            },
        )
        .await?;

    let nodes = storage.get_nodes(None).await?;
    dbg!(&nodes);
    assert_eq!(nodes.iter().find(|n| n.id == "id2").unwrap().active, false);

    Ok(())
}
