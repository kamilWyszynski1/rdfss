// @generated automatically by Diesel CLI.

diesel::table! {
    chunk_locations (rowid) {
        rowid -> Integer,
        chunk_id -> Text,
        node_id -> Text,
    }
}

diesel::table! {
    chunks (id) {
        id -> Text,
        filename -> Text,
    }
}

diesel::table! {
    nodes (id) {
        id -> Text,
        web -> Text,
        rpc -> Text,
        active -> Bool,
    }
}

diesel::joinable!(chunk_locations -> chunks (chunk_id));
diesel::joinable!(chunk_locations -> nodes (node_id));

diesel::allow_tables_to_appear_in_same_query!(
    chunk_locations,
    chunks,
    nodes,
);
