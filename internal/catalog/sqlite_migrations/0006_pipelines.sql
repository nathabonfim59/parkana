CREATE TABLE pipelines (
	"tenant" TEXT NOT NULL,
	"name" TEXT NOT NULL,
	"stage" TEXT NOT NULL,
	"selector" TEXT NOT NULL,
	"spec" BLOB NOT NULL,
	"version" INTEGER NOT NULL,
	"change_token" INTEGER NOT NULL,
	"created_at" INTEGER NOT NULL,
	"updated_at" INTEGER NOT NULL,
	"deleted_at" INTEGER,
	PRIMARY KEY ("tenant", "name")
);
