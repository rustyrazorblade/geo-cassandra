package com.rustyrazorblade.geocassandra

/*

CREATE TABLE device (device text primary key, bloom_filter int)
 WITH compaction = {'class':'LeveledCompactionStrategy'}
 AND caching = {'keys': 'ALL', 'rows_per_partition': 'ALL'}
 AND compression = {'class':'LZ4Compressor', 'chunk_length_kb':4};


CREATE TABLE device_hide (device text, other text, primary key(device, other))
  WITH compaction = {'class':'LeveledCompactionStrategy'}
  AND caching = {'keys': 'ALL', 'rows_per_partition': 'ALL'}
  AND compression = {'class':'LZ4Compressor', 'chunk_length_kb':4};

*/
class Device {

}