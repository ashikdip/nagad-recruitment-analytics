-- ============================================================
-- 01_schema.sql
-- Nagad Recruitment Analytics — Table Definitions (SQLite)
-- Process modelled on Nagad Ltd.'s actual 10-step R&S process
-- ============================================================

DROP TABLE IF EXISTS pipeline_events;
DROP TABLE IF EXISTS candidates;
DROP TABLE IF EXISTS requisitions;

-- ------------------------------------------------------------
-- REQUISITIONS
-- One row per approved Recruitment Requisition Form (RRF).
-- Nagad requires MD approval before any hiring begins.
-- ------------------------------------------------------------
CREATE TABLE requisitions (
    req_id          TEXT PRIMARY KEY,
    job_title       TEXT NOT NULL,
    department      TEXT,
    level           TEXT CHECK(level IN ('Executive','Non-Executive')),
    hire_type       TEXT CHECK(hire_type IN ('Permanent','Contractual')),
    open_date       DATE,
    approved_date   DATE,   -- date MD approved the RRF
    target_hires    INTEGER
);

-- ------------------------------------------------------------
-- CANDIDATES
-- One row per applicant per requisition.
-- ------------------------------------------------------------
CREATE TABLE candidates (
    candidate_id    TEXT PRIMARY KEY,
    req_id          TEXT REFERENCES requisitions(req_id),
    department      TEXT,
    level           TEXT,
    source          TEXT,   -- bdjobs.com | LinkedIn | Internal Referral | Walk-in | Headhunting Firm
    applied_date    DATE,
    final_stage     TEXT,
    outcome         TEXT,
    hired           INTEGER DEFAULT 0,
    days_in_process INTEGER
);

-- ------------------------------------------------------------
-- PIPELINE_EVENTS
-- Event log: one row per stage per candidate.
-- Enables stage-level time and conversion analysis.
-- Stages mirror Nagad's process:
--   Application Received → CV Screening → Written Test →
--   Interview → Reference Check → Offer & Approval → Onboarding
-- ------------------------------------------------------------
CREATE TABLE pipeline_events (
    event_id        INTEGER PRIMARY KEY AUTOINCREMENT,
    candidate_id    TEXT REFERENCES candidates(candidate_id),
    req_id          TEXT REFERENCES requisitions(req_id),
    stage           TEXT,
    entered_date    DATE,
    exited_date     DATE,
    outcome         TEXT
);

CREATE INDEX idx_cand_req     ON candidates(req_id);
CREATE INDEX idx_cand_source  ON candidates(source);
CREATE INDEX idx_cand_outcome ON candidates(outcome);
CREATE INDEX idx_ev_candidate ON pipeline_events(candidate_id);
CREATE INDEX idx_ev_stage     ON pipeline_events(stage);
CREATE INDEX idx_ev_req       ON pipeline_events(req_id);
