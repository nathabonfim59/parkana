CREATE INDEX files_live_time ON files ("tenant", "min_ts", "key") WHERE "deleted_at" IS NULL;
