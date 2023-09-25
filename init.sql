create table if not exists keys (
    key_id integer primary key,
    key text unique,
    length integer not null,
    create_time integer not null default (cast(unixepoch('subsec')*1e3 as integer)),
    last_used integer not null default (cast(unixepoch('subsec')*1e3 as integer)),
    access_count integer not null default 0
) strict;

create table if not exists "values" (
    value_id integer not null references keys(key_id) on delete cascade,
    offset integer not null,
    blob_id integer not null unique,
    primary key (value_id, offset)
) strict;

create table if not exists blobs (
    blob_id integer not null primary key
        references "values"(blob_id) on delete cascade
        -- This lets us create the blob first, then attach it to "values".
        deferrable initially deferred,
    blob blob not null
) strict;

create table if not exists cache_meta (
    key text primary key,
    value
) without rowid;

create index if not exists blob_last_used on keys(last_used, access_count, key_id);

-- While sqlite *seems* to be faster to get sum(length(data)) instead of
-- sum(length(data)), it may still require a large table scan at start-up or with a
-- cold-cache. With this we can be assured that it doesn't.
insert or ignore into cache_meta values ('size', 0);

create table if not exists setting (
    name primary key on conflict replace,
    value
) without rowid;

create table if not exists tags (
    key_id integer references keys(key_id) on delete cascade,
    tag_name any,
    value any,
    primary key (key_id, tag_name)
) strict, without rowid;
