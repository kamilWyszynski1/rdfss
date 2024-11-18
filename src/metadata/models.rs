use diesel::prelude::*;

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

#[derive(Queryable, Insertable, Selectable, Identifiable, Debug, PartialEq, Clone)]
#[diesel(table_name = crate::schema::chunks)]
#[diesel(check_for_backend(diesel::sqlite::Sqlite))]
pub struct Chunk {
    pub id: String,
    pub filename: String,
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
    pub web: String,
}
