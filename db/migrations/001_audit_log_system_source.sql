-- Applied on top of the original schema.sql for an already-running database.
-- Fresh installs get this from schema.sql directly.
--
-- The worker writes audit_log rows for its own actions (recheck resolutions,
-- sweep reclaims, dead-lettering). Those have source 'system'.

BEGIN;

ALTER TABLE audit_log DROP CONSTRAINT audit_log_source_check;

ALTER TABLE audit_log
    ADD CONSTRAINT audit_log_source_check
    CHECK (source IN ('razorpay', 'merchant', 'system'));

COMMIT;
