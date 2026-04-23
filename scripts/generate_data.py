"""
generate_data.py
Generates synthetic recruitment data modelled on Nagad Ltd.'s actual 10-step
Recruitment & Selection Process (documented in BRAC University internship report, 2022).

Three tables:
  - requisitions   : one row per approved hiring request (RRF)
  - candidates     : one row per applicant per requisition
  - pipeline_events: one row per stage transition per candidate (event log)
"""

import pandas as pd
import numpy as np
import random
import sqlite3
import os
from datetime import datetime, timedelta

random.seed(99)
np.random.seed(99)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "..", "data")
os.makedirs(DATA_DIR, exist_ok=True)

START = datetime(2022, 1, 1)
END   = datetime(2023, 12, 31)

# ── REFERENCE DATA ─────────────────────────────────────────────────────────────
DEPARTMENTS = [
    "Technology", "Finance", "Marketing", "Operations",
    "Customer Service", "Agent Banking", "Product", "HR", "Legal & Compliance"
]

# Weights: high-volume hiring in Tech, Ops, Customer Service, Agent Banking
DEPT_WEIGHTS = [0.20, 0.08, 0.10, 0.18, 0.15, 0.14, 0.07, 0.05, 0.03]

LEVELS = ["Non-Executive", "Executive"]
LEVEL_WEIGHTS = [0.65, 0.35]   # more non-exec hires

SOURCES = ["bdjobs.com", "LinkedIn", "Internal Referral", "Walk-in", "Headhunting Firm"]
# Report notes bdjobs + LinkedIn as primary channels
SOURCE_WEIGHTS = [0.40, 0.28, 0.18, 0.08, 0.06]

JOB_TITLES = {
    "Technology":         ["Software Engineer", "QA Engineer", "DevOps Engineer",
                           "Data Analyst", "System Administrator", "Tech Lead"],
    "Finance":            ["Financial Analyst", "Accounts Officer", "Treasury Analyst",
                           "Finance Manager"],
    "Marketing":          ["Digital Marketing Executive", "Brand Executive",
                           "Content Writer", "Marketing Manager"],
    "Operations":         ["Operations Executive", "Process Analyst",
                           "Field Coordinator", "Operations Manager"],
    "Customer Service":   ["Customer Service Executive", "CS Team Lead",
                           "Call Centre Agent", "CS Manager"],
    "Agent Banking":      ["Agent Network Executive", "Agent Relationship Officer",
                           "Area Manager - Agent Banking"],
    "Product":            ["Product Analyst", "Product Manager", "UX Designer",
                           "Business Analyst"],
    "HR":                 ["HR Executive", "Talent Acquisition Specialist",
                           "HR Business Partner", "HR Manager"],
    "Legal & Compliance": ["Compliance Officer", "Legal Executive", "Risk Analyst"],
}

REJECTION_REASONS = {
    "cv_screen":      ["Under-qualified", "Over-qualified", "Incomplete CV",
                       "Not matching JD", "Duplicate application"],
    "written_test":   ["Below cutoff score", "Failed aptitude", "Absent"],
    "interview":      ["Poor communication", "Culture fit mismatch",
                       "Salary expectation too high", "Weak technical skills",
                       "Joined competitor before interview"],
    "offer":          ["Accepted competitor offer", "Counter-offer from current employer",
                       "Salary mismatch", "Relocation issue"],
}

# ── NAGAD PROCESS STAGES ───────────────────────────────────────────────────────
# Stage name → (min_days, max_days) at that stage
# Based on report: small team of 3, deadline pressure, prolonged onboarding
STAGE_DURATIONS = {
    "Application Received":  (0, 1),
    "CV Screening":          (3, 10),    # batch processing
    "Written Test":          (5, 14),    # scheduling delays
    "Interview":             (7, 21),    # 3-level for Executive; 1-level for Non-Exec
    "Reference Check":       (3, 10),
    "Offer & Approval":      (5, 15),    # needs MD + HR Director sign-off
    "Onboarding":            (7, 30),    # report flags prolonged onboarding
}

# ── FUNNEL CONVERSION RATES ────────────────────────────────────────────────────
# Grounded in report findings: high application volume, small screening team
FUNNEL = {
    # (pass_rate_non_exec, pass_rate_exec)
    "cv_screen":      (0.28, 0.22),
    "written_test":   (0.55, 0.45),
    "interview":      (0.62, 0.50),
    "reference":      (0.88, 0.85),
    "offer_accept":   (0.78, 0.72),
}

# ── 1. REQUISITIONS ────────────────────────────────────────────────────────────
N_REQS = 130   # ~65/year for a 700-person company with 3 TA staff

reqs = []
for i in range(1, N_REQS + 1):
    dept      = random.choices(DEPARTMENTS, weights=DEPT_WEIGHTS)[0]
    level     = random.choices(LEVELS, weights=LEVEL_WEIGHTS)[0]
    title     = random.choice(JOB_TITLES[dept])
    if level == "Executive" and "Manager" not in title:
        title = title  # keep as-is; some are exec-track
    open_date    = START + timedelta(days=random.randint(0, (END - START).days - 60))
    rrf_days     = random.randint(3, 10)   # time from need to MD approval
    approved_date = open_date + timedelta(days=rrf_days)
    target_hires  = random.choices([1, 2, 3], weights=[0.60, 0.30, 0.10])[0]
    hire_type     = random.choices(["Permanent", "Contractual"], weights=[0.72, 0.28])[0]

    reqs.append({
        "req_id":          f"REQ{i:04d}",
        "job_title":       title,
        "department":      dept,
        "level":           level,
        "hire_type":       hire_type,
        "open_date":       open_date.strftime("%Y-%m-%d"),
        "approved_date":   approved_date.strftime("%Y-%m-%d"),
        "target_hires":    target_hires,
    })

reqs_df = pd.DataFrame(reqs)

# ── 2. CANDIDATES + 3. PIPELINE EVENTS ────────────────────────────────────────
candidates = []
events     = []
cid        = 1

for _, req in reqs_df.iterrows():
    level      = req["level"]
    approved   = datetime.strptime(req["approved_date"], "%Y-%m-%d")

    # Applications arrive over 7–21 days after approval
    n_apps = random.randint(25, 120)  # high volume per report

    for _ in range(n_apps):
        source     = random.choices(SOURCES, weights=SOURCE_WEIGHTS)[0]
        apply_days = random.randint(1, 21)
        apply_date = approved + timedelta(days=apply_days)

        if apply_date > END:
            continue

        c_id   = f"CAN{cid:05d}"
        cid   += 1
        outcome = "Active"
        current_stage = "Application Received"
        stage_date    = apply_date
        hired         = False

        ev_list = []
        ev_list.append({
            "candidate_id": c_id,
            "req_id":       req["req_id"],
            "stage":        "Application Received",
            "entered_date": apply_date.strftime("%Y-%m-%d"),
            "exited_date":  apply_date.strftime("%Y-%m-%d"),
            "outcome":      "Passed",
        })

        # CV Screen
        pass_rate = FUNNEL["cv_screen"][0 if level == "Non-Executive" else 1]
        dur = random.randint(*STAGE_DURATIONS["CV Screening"])
        exit_date = apply_date + timedelta(days=dur)
        passed = random.random() < pass_rate
        ev_list.append({
            "candidate_id": c_id,
            "req_id":       req["req_id"],
            "stage":        "CV Screening",
            "entered_date": apply_date.strftime("%Y-%m-%d"),
            "exited_date":  exit_date.strftime("%Y-%m-%d"),
            "outcome":      "Passed" if passed else random.choice(REJECTION_REASONS["cv_screen"]),
        })
        if not passed:
            outcome = "Rejected - CV Screen"
            current_stage = "CV Screening"
        else:
            # Written Test
            prev = exit_date
            pass_rate2 = FUNNEL["written_test"][0 if level == "Non-Executive" else 1]
            dur2 = random.randint(*STAGE_DURATIONS["Written Test"])
            exit2 = prev + timedelta(days=dur2)
            passed2 = random.random() < pass_rate2
            ev_list.append({
                "candidate_id": c_id,
                "req_id":       req["req_id"],
                "stage":        "Written Test",
                "entered_date": prev.strftime("%Y-%m-%d"),
                "exited_date":  exit2.strftime("%Y-%m-%d"),
                "outcome":      "Passed" if passed2 else random.choice(REJECTION_REASONS["written_test"]),
            })
            if not passed2:
                outcome = "Rejected - Written Test"
                current_stage = "Written Test"
            else:
                # Interview
                prev = exit2
                pass_rate3 = FUNNEL["interview"][0 if level == "Non-Executive" else 1]
                dur3 = random.randint(*STAGE_DURATIONS["Interview"])
                exit3 = prev + timedelta(days=dur3)
                # Candidate dropout: higher for exec due to longer timeline
                dropout_rate = 0.08 if level == "Non-Executive" else 0.14
                if random.random() < dropout_rate:
                    ev_list.append({
                        "candidate_id": c_id,
                        "req_id":       req["req_id"],
                        "stage":        "Interview",
                        "entered_date": prev.strftime("%Y-%m-%d"),
                        "exited_date":  exit3.strftime("%Y-%m-%d"),
                        "outcome":      "Withdrew - Joined Competitor",
                    })
                    outcome = "Withdrew"
                    current_stage = "Interview"
                else:
                    passed3 = random.random() < pass_rate3
                    ev_list.append({
                        "candidate_id": c_id,
                        "req_id":       req["req_id"],
                        "stage":        "Interview",
                        "entered_date": prev.strftime("%Y-%m-%d"),
                        "exited_date":  exit3.strftime("%Y-%m-%d"),
                        "outcome":      "Passed" if passed3 else random.choice(REJECTION_REASONS["interview"]),
                    })
                    if not passed3:
                        outcome = "Rejected - Interview"
                        current_stage = "Interview"
                    else:
                        # Reference Check
                        prev = exit3
                        pass_rate4 = FUNNEL["reference"][0 if level == "Non-Executive" else 1]
                        dur4 = random.randint(*STAGE_DURATIONS["Reference Check"])
                        exit4 = prev + timedelta(days=dur4)
                        passed4 = random.random() < pass_rate4
                        ev_list.append({
                            "candidate_id": c_id,
                            "req_id":       req["req_id"],
                            "stage":        "Reference Check",
                            "entered_date": prev.strftime("%Y-%m-%d"),
                            "exited_date":  exit4.strftime("%Y-%m-%d"),
                            "outcome":      "Passed" if passed4 else "Failed Reference",
                        })
                        if not passed4:
                            outcome = "Rejected - Reference"
                            current_stage = "Reference Check"
                        else:
                            # Offer
                            prev = exit4
                            accept_rate = FUNNEL["offer_accept"][0 if level == "Non-Executive" else 1]
                            dur5 = random.randint(*STAGE_DURATIONS["Offer & Approval"])
                            exit5 = prev + timedelta(days=dur5)
                            accepted = random.random() < accept_rate
                            ev_list.append({
                                "candidate_id": c_id,
                                "req_id":       req["req_id"],
                                "stage":        "Offer & Approval",
                                "entered_date": prev.strftime("%Y-%m-%d"),
                                "exited_date":  exit5.strftime("%Y-%m-%d"),
                                "outcome":      "Accepted" if accepted else random.choice(REJECTION_REASONS["offer"]),
                            })
                            if accepted:
                                outcome = "Hired"
                                current_stage = "Hired"
                                hired = True
                                # Onboarding
                                dur6 = random.randint(*STAGE_DURATIONS["Onboarding"])
                                exit6 = exit5 + timedelta(days=dur6)
                                ev_list.append({
                                    "candidate_id": c_id,
                                    "req_id":       req["req_id"],
                                    "stage":        "Onboarding",
                                    "entered_date": exit5.strftime("%Y-%m-%d"),
                                    "exited_date":  exit6.strftime("%Y-%m-%d"),
                                    "outcome":      "Completed",
                                })
                            else:
                                outcome = "Offer Declined"
                                current_stage = "Offer & Approval"

        # Compute total days in process
        first_ev = ev_list[0]["entered_date"]
        last_ev  = ev_list[-1]["exited_date"]
        days_in_process = (datetime.strptime(last_ev, "%Y-%m-%d") -
                           datetime.strptime(first_ev, "%Y-%m-%d")).days

        candidates.append({
            "candidate_id":    c_id,
            "req_id":          req["req_id"],
            "department":      req["department"],
            "level":           level,
            "source":          source,
            "applied_date":    apply_date.strftime("%Y-%m-%d"),
            "final_stage":     current_stage,
            "outcome":         outcome,
            "hired":           int(hired),
            "days_in_process": days_in_process,
        })
        events.extend(ev_list)

cands_df  = pd.DataFrame(candidates)
events_df = pd.DataFrame(events)

# ── SAVE ───────────────────────────────────────────────────────────────────────
reqs_df.to_csv(os.path.join(DATA_DIR, "requisitions.csv"),    index=False)
cands_df.to_csv(os.path.join(DATA_DIR, "candidates.csv"),     index=False)
events_df.to_csv(os.path.join(DATA_DIR, "pipeline_events.csv"), index=False)

db = sqlite3.connect(os.path.join(DATA_DIR, "nagad_recruitment.db"))
reqs_df.to_sql("requisitions",    db, if_exists="replace", index=False)
cands_df.to_sql("candidates",     db, if_exists="replace", index=False)
events_df.to_sql("pipeline_events", db, if_exists="replace", index=False)
db.close()

print(f"✓ requisitions.csv    → {len(reqs_df):,} rows")
print(f"✓ candidates.csv      → {len(cands_df):,} rows")
print(f"✓ pipeline_events.csv → {len(events_df):,} rows")
print(f"✓ nagad_recruitment.db created")
print(f"\n  Total hires in dataset : {cands_df['hired'].sum():,}")
print(f"  Overall hire rate      : {cands_df['hired'].mean()*100:.1f}%")
