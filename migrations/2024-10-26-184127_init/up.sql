-- Your SQL goes here
create table if not exists nodes
(
    id     text    not null primary key,
    web    text    not null,
    rpc    text    not null,
    active boolean not null
);

create table if not exists chunks
(
    id       text not null primary key,
    filename text not null
);

create table if not exists chunk_locations
(
    chunk_id text not null,
    node_id  text not null,
    foreign key (chunk_id) references chunks (id),
    foreign key (node_id) references nodes (id)
);