-- We have to opt into this before creating any tables, or before a vacuum to enable it. It means we
-- can trim the database file size with partial vacuums without having to do a full vacuum, which
-- locks everything.
pragma auto_vacuum=incremental;

create table if not exists blob (
    name text primary key,
    last_used timestamp default (datetime('now')),
    data_id integer not null references blob_data(data_id) on delete cascade
) without rowid;

create table if not exists blob_data (
    data_id integer primary key,
    data blob not null
) strict;

create table if not exists blob_meta (
    key text primary key,
    value
) without rowid;

create index if not exists blob_last_used on blob(last_used);

-- While sqlite *seems* to be faster to get sum(length(data)) instead of
-- sum(length(data)), it may still require a large table scan at start-up or with a
-- cold-cache. With this we can be assured that it doesn't.
insert or ignore into blob_meta values ('size', 0);

create table if not exists setting (
    name primary key on conflict replace,
    value
) without rowid;

create table if not exists tag (
    blob_name references blob(name) on delete cascade,
    tag_name,
    value,
    primary key (blob_name, tag_name)
) without rowid;

create view if not exists deletable_blob as
with recursive excess (
    usage_with,
    last_used,
    blob_data_id,
    data_length
) as (
    select *
    from (
        select
            (select value from blob_meta where key='size') as usage_with,
            last_used,
            data_id,
            length(data)
        from blob join blob_data using (data_id) order by last_used, data_id limit 1
    )
    where usage_with > (select value from setting where name='capacity')
    union all
    select
        usage_with-data_length as new_usage_with,
        blob.last_used,
        blob.data_id,
        length(data)
    from excess join blob
    on blob.data_id=(select data_id from blob where (last_used, data_id) > (excess.last_used, blob_data_id))
    join blob_data using (data_id)
    where new_usage_with > (select value from setting where name='capacity')
)
select * from excess;
