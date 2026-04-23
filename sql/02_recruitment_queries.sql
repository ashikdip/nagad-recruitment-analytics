-- ============================================================
-- 02_recruitment_queries.sql
-- Nagad Recruitment Efficiency Analytics — Business Queries
-- Run against: data/nagad_recruitment.db (SQLite)
-- ============================================================


-- ── Q1: Overall Recruitment KPIs ─────────────────────────────────────────────
SELECT
    COUNT(DISTINCT r.req_id)                                AS total_requisitions,
    SUM(r.target_hires)                                     AS total_positions_open,
    COUNT(DISTINCT c.candidate_id)                          AS total_applicants,
    SUM(c.hired)                                            AS total_hired,
    ROUND(100.0 * SUM(c.hired) / COUNT(c.candidate_id), 2) AS overall_hire_rate_pct,
    ROUND(AVG(CASE WHEN c.hired = 1 THEN c.days_in_process END), 1) AS avg_time_to_hire_days
FROM requisitions r
LEFT JOIN candidates c ON r.req_id = c.req_id;


-- ── Q2: Recruitment Funnel — Stage-by-Stage Conversion ───────────────────────
-- Shows how many candidates enter each stage and how many pass
SELECT
    stage,
    COUNT(*)                                                    AS entered,
    SUM(CASE WHEN outcome = 'Passed' OR outcome = 'Accepted'
                  OR outcome = 'Completed' THEN 1 ELSE 0 END)  AS passed,
    ROUND(100.0 *
        SUM(CASE WHEN outcome = 'Passed' OR outcome = 'Accepted'
                      OR outcome = 'Completed' THEN 1 ELSE 0 END)
        / COUNT(*), 2)                                          AS pass_rate_pct
FROM pipeline_events
GROUP BY stage
ORDER BY
    CASE stage
        WHEN 'Application Received' THEN 1
        WHEN 'CV Screening'         THEN 2
        WHEN 'Written Test'         THEN 3
        WHEN 'Interview'            THEN 4
        WHEN 'Reference Check'      THEN 5
        WHEN 'Offer & Approval'     THEN 6
        WHEN 'Onboarding'           THEN 7
    END;


-- ── Q3: Average Days Spent at Each Stage (Time-to-Fill Breakdown) ────────────
-- Pinpoints which stage creates the most delay
SELECT
    stage,
    ROUND(AVG(julianday(exited_date) - julianday(entered_date)), 1) AS avg_days,
    ROUND(MIN(julianday(exited_date) - julianday(entered_date)), 0) AS min_days,
    ROUND(MAX(julianday(exited_date) - julianday(entered_date)), 0) AS max_days,
    COUNT(*) AS volume
FROM pipeline_events
GROUP BY stage
ORDER BY
    CASE stage
        WHEN 'Application Received' THEN 1
        WHEN 'CV Screening'         THEN 2
        WHEN 'Written Test'         THEN 3
        WHEN 'Interview'            THEN 4
        WHEN 'Reference Check'      THEN 5
        WHEN 'Offer & Approval'     THEN 6
        WHEN 'Onboarding'           THEN 7
    END;


-- ── Q4: Source Channel Effectiveness ─────────────────────────────────────────
-- Which channel delivers the most hires and best conversion?
SELECT
    c.source,
    COUNT(*)                                                        AS applicants,
    SUM(c.hired)                                                    AS hires,
    ROUND(100.0 * SUM(c.hired) / COUNT(*), 2)                      AS hire_rate_pct,
    ROUND(AVG(CASE WHEN c.hired = 1 THEN c.days_in_process END), 1) AS avg_days_to_hire
FROM candidates c
GROUP BY c.source
ORDER BY hires DESC;


-- ── Q5: Hiring by Department ─────────────────────────────────────────────────
SELECT
    department,
    COUNT(DISTINCT req_id)                                          AS requisitions,
    COUNT(*)                                                        AS applicants,
    SUM(hired)                                                      AS hires,
    ROUND(100.0 * SUM(hired) / COUNT(*), 2)                        AS hire_rate_pct,
    ROUND(AVG(CASE WHEN hired = 1 THEN days_in_process END), 1)    AS avg_days_to_hire
FROM candidates
GROUP BY department
ORDER BY hires DESC;


-- ── Q6: Executive vs Non-Executive Funnel Comparison ────────────────────────
SELECT
    c.level,
    pe.stage,
    COUNT(*)                                                       AS entered,
    ROUND(100.0 *
        SUM(CASE WHEN pe.outcome IN ('Passed','Accepted','Completed')
                 THEN 1 ELSE 0 END) / COUNT(*), 2)                AS pass_rate_pct,
    ROUND(AVG(julianday(pe.exited_date) - julianday(pe.entered_date)), 1) AS avg_days
FROM pipeline_events pe
JOIN candidates c ON pe.candidate_id = c.candidate_id
GROUP BY c.level, pe.stage
ORDER BY c.level,
    CASE pe.stage
        WHEN 'Application Received' THEN 1
        WHEN 'CV Screening'         THEN 2
        WHEN 'Written Test'         THEN 3
        WHEN 'Interview'            THEN 4
        WHEN 'Reference Check'      THEN 5
        WHEN 'Offer & Approval'     THEN 6
        WHEN 'Onboarding'           THEN 7
    END;


-- ── Q7: Candidate Dropout — Withdrew to Join Competitor ──────────────────────
-- Validates report finding: candidates lost during long process
SELECT
    c.level,
    c.department,
    COUNT(*)   AS withdrew_count,
    ROUND(100.0 * COUNT(*) /
        (SELECT COUNT(*) FROM candidates c2
         WHERE c2.department = c.department), 2) AS pct_of_dept_applicants
FROM candidates c
WHERE c.outcome = 'Withdrew'
GROUP BY c.level, c.department
ORDER BY withdrew_count DESC;


-- ── Q8: Offer Acceptance Rate by Department and Level ────────────────────────
SELECT
    c.department,
    c.level,
    COUNT(*)                                                                    AS offers_made,
    SUM(CASE WHEN c.outcome = 'Hired' THEN 1 ELSE 0 END)                       AS accepted,
    SUM(CASE WHEN c.outcome = 'Offer Declined' THEN 1 ELSE 0 END)              AS declined,
    ROUND(100.0 * SUM(CASE WHEN c.outcome = 'Hired' THEN 1 ELSE 0 END)
          / COUNT(*), 2)                                                        AS acceptance_rate_pct
FROM candidates c
WHERE c.final_stage = 'Offer & Approval'
  AND c.outcome IN ('Hired', 'Offer Declined')
GROUP BY c.department, c.level
ORDER BY acceptance_rate_pct DESC;


-- ── Q9: Monthly Hiring Volume (2022 vs 2023) ─────────────────────────────────
SELECT
    strftime('%Y', applied_date) AS year,
    strftime('%m', applied_date) AS month,
    COUNT(*)                     AS applications,
    SUM(hired)                   AS hires
FROM candidates
GROUP BY year, month
ORDER BY year, month;


-- ── Q10: Top Rejection Reasons ───────────────────────────────────────────────
SELECT
    outcome                        AS rejection_reason,
    COUNT(*)                       AS count,
    ROUND(100.0 * COUNT(*) /
        (SELECT COUNT(*) FROM pipeline_events
         WHERE outcome NOT IN ('Passed','Accepted','Completed')), 2) AS pct_of_all_rejections
FROM pipeline_events
WHERE outcome NOT IN ('Passed','Accepted','Completed')
GROUP BY outcome
ORDER BY count DESC
LIMIT 12;
