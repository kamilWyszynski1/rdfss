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

#[derive(
    Queryable, Insertable, Selectable, Identifiable, Associations, Debug, PartialEq, Clone,
)]
#[diesel(table_name = crate::schema::chunks)]
#[diesel(belongs_to(Node))]
#[diesel(check_for_backend(diesel::sqlite::Sqlite))]
pub struct Chunk {
    pub id: String,
    pub filename: String,
    pub node_id: String,
}
