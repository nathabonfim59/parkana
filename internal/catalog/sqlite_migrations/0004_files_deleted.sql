CREATE INDEX files_deleted ON files ("tenant", "deleted_at") WHERE "deleted_at" IS NOT NULL;
