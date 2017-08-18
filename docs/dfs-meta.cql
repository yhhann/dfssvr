-- The option of replication will be adjusted according to ops.
CREATE KEYSPACE dfs WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 3};

CREATE TABLE dupl (
	id text,
	refid text,
	size bigint,
	domain bigint,
	cdate timestamp,
	PRIMARY KEY(id)
);

CREATE TABLE rc (
	id text,
	refcnt counter,
	PRIMARY KEY(id)
);

CREATE TABLE files (
	id text,
	domain bigint,
	size bigint,
	md5 text,
	cksize int,
	biz text,
	fn text,
	udate timestamp,
	uid text,
	attrs map<text, text>,
	type tinyint,
	PRIMARY KEY(id)
);

CREATE MATERIALIZED VIEW md5 AS
  SELECT * FROM files
  WHERE id IS NOT NULL AND md5 IS NOT NULL
  PRIMARY KEY (md5, id);
