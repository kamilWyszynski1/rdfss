use chrono::{NaiveDateTime, Utc};
use derive_builder::Builder;
use diesel::prelude::*;

#[derive(Queryable, Identifiable, Insertable, Selectable, PartialEq, Debug, Clone)]
#[diesel(table_name = crate::schema::files)]
#[diesel(check_for_backend(diesel::sqlite::Sqlite))]
pub struct File {
    pub id: String,
    pub name: String,
    pub created_at: NaiveDateTime,
    pub modified_at: NaiveDateTime,
    pub replication_factor: i32, // database default is 3
}

impl File {
    pub fn new(id: String, name: String) -> Self {
        Self {
            id,
            name,
            created_at: Utc::now().naive_utc(),
            modified_at: Utc::now().naive_utc(),
            replication_factor: 3,
        }
    }
}

#[derive(Queryable, Identifiable, Insertable, Selectable, PartialEq, Debug, Clone)]
#[diesel(table_name = crate::schema::nodes)]
#[diesel(check_for_backend(diesel::sqlite::Sqlite))]
pub struct Node {
    pub id: String,
    pub web: String,
    pub rpc: String,
    pub active: bool,
}

#[derive(AsChangeset)]
#[diesel(table_name = crate::schema::nodes)]
#[diesel(check_for_backend(diesel::sqlite::Sqlite))]
pub struct NodeUpdate {
    pub active: Option<bool>,
}

#[derive(Queryable, Insertable, Selectable, Identifiable, Debug, PartialEq, Clone, Hash, Eq)]
#[diesel(table_name = crate::schema::chunks)]
#[diesel(check_for_backend(diesel::sqlite::Sqlite))]
pub struct Chunk {
    pub id: String,
    pub file_id: String,
    pub chunk_index: i32,
}

#[derive(Queryable, Insertable, Selectable, Associations, Debug, PartialEq, Clone)]
#[diesel(belongs_to(Node))]
#[diesel(belongs_to(Chunk, foreign_key = chunk_id))]
#[diesel(table_name = crate::schema::chunk_locations)]
#[diesel(check_for_backend(diesel::sqlite::Sqlite))]
pub struct ChunkLocation {
    pub chunk_id: String,
    pub node_id: String,
}

#[derive(Debug, Queryable, PartialEq)]
pub struct ChunkWithWeb {
    pub chunk_id: String,
    pub chunk_index: i32,
    pub web: String,
}

#[derive(Builder, Debug, Default, Clone, Queryable, PartialEq)]
pub struct ChunksQuery {
    #[builder(setter(into, strip_option), default)]
    pub file_id: Option<String>,
    #[builder(setter(into, strip_option), default)]
    pub active_node: Option<bool>,
}
