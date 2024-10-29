// @generated automatically by Diesel CLI.

diesel::table! {
    chunks (id) {
        id -> Text,
        filename -> Text,
        node_id -> Text,
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

diesel::joinable!(chunks -> nodes (node_id));

diesel::allow_tables_to_appear_in_same_query!(
    chunks,
    nodes,
);
