CREATE TABLE device (device text primary key, bloom_filter int) WITH compaction = {'class':'LeveledCompactionStrategy'} AND caching = {'keys': 'ALL', 'rows_per_partition': 'ALL'} AND compression = {'class':'LZ4Compressor', 'chunk_length_kb':4};


CREATE TABLE device_ignore (device text, other text, primary key(device, other)) WITH compaction = {'class':'LeveledCompactionStrategy'} AND caching = {'keys': 'ALL', 'rows_per_partition': 'ALL'}  AND compression = {'class':'LZ4Compressor', 'chunk_length_kb':4};

create table location_updates ( geohash text, device text, lat double, long double, primary key (geohash, device ) );

CREATE table ignore_stats ( device text primary key, num counter )   WITH compaction = {'class':'LeveledCompactionStrategy'}  AND caching = {'keys': 'ALL', 'rows_per_partition': 'ALL'}  AND compression = {'class':'LZ4Compressor', 'chunk_length_kb':4};