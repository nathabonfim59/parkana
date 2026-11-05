CREATE TABLE files (
	"tenant" TEXT NOT NULL,
	"key" TEXT NOT NULL,
	"stream" TEXT NOT NULL,
	"min_ts" INTEGER NOT NULL,
	"max_ts" INTEGER NOT NULL,
	"rows" INTEGER NOT NULL,
	"bytes" INTEGER NOT NULL,
	"label_values" TEXT NOT NULL,
	"has_sidecar" INTEGER NOT NULL,
	"created_at" INTEGER NOT NULL,
	"deleted_at" INTEGER,
	PRIMARY KEY ("tenant", "key")
);
