-- Your SQL goes here
create table if not exists files
(
    id                 text     not null primary key,
    name               text     not null,
    created_at         datetime not null default current_timestamp,
    modified_at        datetime not null default current_timestamp,
    replication_factor int      not null default 3
);

create table if not exists nodes
(
    id     text    not null primary key,
    web    text    not null,
    rpc    text    not null,
    active boolean not null
);

create table if not exists chunks
(
    id          text not null primary key,
    file_id     text not null,
    chunk_index int  not null,

    foreign key (file_id) references files (id)
);

create table if not exists chunk_locations
(
    chunk_id text not null,
    node_id  text not null,

    foreign key (chunk_id) references chunks (id),
    foreign key (node_id) references nodes (id)
);