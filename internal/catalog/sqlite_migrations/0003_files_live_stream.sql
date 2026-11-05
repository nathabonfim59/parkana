CREATE INDEX files_live_stream ON files ("tenant", "stream") WHERE "deleted_at" IS NULL;
