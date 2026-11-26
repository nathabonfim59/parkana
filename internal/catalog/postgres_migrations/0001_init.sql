-- The Postgres catalog schema, equal to SQLite's after its migration 0006.
-- Text columns that order or identify rows use COLLATE "C" so comparisons are
-- bytewise, like Go and SQLite. Timestamps are BIGINT Unix microseconds.
CREATE TABLE files (
	"tenant" TEXT COLLATE "C" NOT NULL,
	"key" TEXT COLLATE "C" NOT NULL,
	"stream" TEXT COLLATE "C" NOT NULL,
	"min_ts" BIGINT NOT NULL,
	"max_ts" BIGINT NOT NULL,
	"rows" BIGINT NOT NULL,
	"bytes" BIGINT NOT NULL,
	"label_values" TEXT NOT NULL,
	"has_sidecar" BOOLEAN NOT NULL DEFAULT FALSE,
	"created_at" BIGINT NOT NULL,
	"deleted_at" BIGINT,
	"labels_complete" BOOLEAN NOT NULL DEFAULT FALSE,
	PRIMARY KEY ("tenant", "key")
);

CREATE INDEX files_live_time ON files ("tenant", "min_ts", "key") WHERE "deleted_at" IS NULL;
CREATE INDEX files_live_stream ON files ("tenant", "stream") WHERE "deleted_at" IS NULL;
CREATE INDEX files_deleted ON files ("tenant", "deleted_at") WHERE "deleted_at" IS NOT NULL;

CREATE TABLE pipelines (
	"tenant" TEXT COLLATE "C" NOT NULL,
	"name" TEXT COLLATE "C" NOT NULL,
	"stage" TEXT NOT NULL,
	"selector" TEXT NOT NULL,
	"spec" BYTEA NOT NULL,
	"version" BIGINT NOT NULL,
	"change_token" BIGINT NOT NULL,
	"created_at" BIGINT NOT NULL,
	"updated_at" BIGINT NOT NULL,
	"deleted_at" BIGINT,
	PRIMARY KEY ("tenant", "name")
);
