"""
analysis.py
Nagad Recruitment Efficiency Analytics — EDA + Charts
Produces 7 publication-quality charts saved to /visuals/
"""

import sqlite3, os
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import matplotlib.patches as mpatches
import seaborn as sns
import numpy as np

BASE_DIR   = os.path.dirname(os.path.abspath(__file__))
DATA_DIR   = os.path.join(BASE_DIR, "..", "data")
VIS_DIR    = os.path.join(BASE_DIR, "..", "visuals")
os.makedirs(VIS_DIR, exist_ok=True)

conn = sqlite3.connect(os.path.join(DATA_DIR, "nagad_recruitment.db"))
sns.set_theme(style="whitegrid", palette="deep")
plt.rcParams.update({"figure.dpi": 150, "font.size": 11})
C = ["#1D4ED8","#16A34A","#DC2626","#D97706","#7C3AED","#0891B2","#BE185D"]

STAGE_ORDER = ["Application Received","CV Screening","Written Test",
               "Interview","Reference Check","Offer & Approval","Onboarding"]

def save(fig, name):
    fig.savefig(os.path.join(VIS_DIR, name), bbox_inches="tight")
    plt.close(fig)
    print(f"  saved → visuals/{name}")

print("Generating charts...")

# ── CHART 1: Recruitment Funnel ───────────────────────────────────────────────
funnel = pd.read_sql("""
    SELECT stage,
           COUNT(*) AS entered,
           SUM(CASE WHEN outcome IN ('Passed','Accepted','Completed') THEN 1 ELSE 0 END) AS passed
    FROM pipeline_events GROUP BY stage
""", conn)
funnel["stage_order"] = funnel["stage"].map({s:i for i,s in enumerate(STAGE_ORDER)})
funnel = funnel.sort_values("stage_order")

fig, ax = plt.subplots(figsize=(12, 6))
bars = ax.barh(funnel["stage"], funnel["entered"], color=C[0], alpha=0.4, label="Entered")
ax.barh(funnel["stage"], funnel["passed"], color=C[0], label="Passed / Hired")
ax.invert_yaxis()
for i, (e, p) in enumerate(zip(funnel["entered"], funnel["passed"])):
    pct = p/e*100 if e > 0 else 0
    ax.text(e + 30, i, f"{e:,}  →  {p:,}  ({pct:.0f}%)", va="center", fontsize=9)
ax.set_xlabel("Candidates")
ax.set_title("Nagad Recruitment Funnel — Stage-by-Stage Conversion", fontweight="bold", pad=12)
ax.legend()
ax.set_xlim(0, funnel["entered"].max() * 1.35)
save(fig, "01_recruitment_funnel.png")

# ── CHART 2: Average Days at Each Stage ──────────────────────────────────────
stage_time = pd.read_sql("""
    SELECT stage,
           ROUND(AVG(julianday(exited_date)-julianday(entered_date)),1) AS avg_days
    FROM pipeline_events GROUP BY stage
""", conn)
stage_time["stage_order"] = stage_time["stage"].map({s:i for i,s in enumerate(STAGE_ORDER)})
stage_time = stage_time.sort_values("stage_order")

fig, ax = plt.subplots(figsize=(10, 5))
colors = [C[2] if d > 12 else C[1] for d in stage_time["avg_days"]]
bars = ax.bar(stage_time["stage"], stage_time["avg_days"], color=colors, edgecolor="white")
ax.bar_label(bars, labels=[f"{v:.1f}d" for v in stage_time["avg_days"]], padding=4)
ax.set_ylabel("Average Days")
ax.set_title("Average Time Spent at Each Recruitment Stage", fontweight="bold", pad=12)
plt.xticks(rotation=30, ha="right")
red_p  = mpatches.Patch(color=C[2], label="Bottleneck (>12 days)")
grn_p  = mpatches.Patch(color=C[1], label="Acceptable")
ax.legend(handles=[red_p, grn_p])
save(fig, "02_stage_duration.png")

# ── CHART 3: Source Channel Effectiveness ────────────────────────────────────
src = pd.read_sql("""
    SELECT source,
           COUNT(*) AS applicants,
           SUM(hired) AS hires,
           ROUND(100.0*SUM(hired)/COUNT(*),2) AS hire_rate_pct,
           ROUND(AVG(CASE WHEN hired=1 THEN days_in_process END),1) AS avg_days
    FROM candidates GROUP BY source ORDER BY hires DESC
""", conn)

fig, ax1 = plt.subplots(figsize=(10, 5))
ax2 = ax1.twinx()
x = range(len(src))
ax1.bar(x, src["applicants"], color=C[0], alpha=0.6, label="Applicants", width=0.4, align="center")
ax1.bar([i+0.4 for i in x], src["hires"],      color=C[1], alpha=0.85, label="Hires",      width=0.4)
ax2.plot([i+0.2 for i in x], src["hire_rate_pct"], color=C[2], marker="D",
         markersize=9, linewidth=2.5, label="Hire Rate %")
ax1.set_xticks([i+0.2 for i in x])
ax1.set_xticklabels(src["source"], rotation=20, ha="right")
ax1.set_ylabel("Count")
ax2.set_ylabel("Hire Rate (%)", color=C[2])
ax1.set_title("Source Channel: Volume vs Hire Rate", fontweight="bold", pad=12)
lines1, labels1 = ax1.get_legend_handles_labels()
lines2, labels2 = ax2.get_legend_handles_labels()
ax1.legend(lines1+lines2, labels1+labels2, loc="upper right")
save(fig, "03_source_effectiveness.png")

# ── CHART 4: Time-to-Hire by Department ──────────────────────────────────────
dept_ttf = pd.read_sql("""
    SELECT department,
           ROUND(AVG(CASE WHEN hired=1 THEN days_in_process END),1) AS avg_days_to_hire,
           SUM(hired) AS hires
    FROM candidates GROUP BY department ORDER BY avg_days_to_hire DESC
""", conn)
dept_ttf = dept_ttf.dropna(subset=["avg_days_to_hire"])

fig, ax = plt.subplots(figsize=(10, 5))
colors = [C[2] if d > 50 else C[0] for d in dept_ttf["avg_days_to_hire"]]
bars = ax.barh(dept_ttf["department"], dept_ttf["avg_days_to_hire"], color=colors)
ax.bar_label(bars, labels=[f"{v:.0f} days" for v in dept_ttf["avg_days_to_hire"]], padding=4)
ax.set_xlabel("Avg Days to Hire")
ax.set_title("Average Time-to-Hire by Department", fontweight="bold", pad=12)
ax.invert_yaxis()
save(fig, "04_time_to_hire_dept.png")

# ── CHART 5: Executive vs Non-Executive Pass Rates ───────────────────────────
exec_funnel = pd.read_sql("""
    SELECT c.level, pe.stage,
           ROUND(100.0*SUM(CASE WHEN pe.outcome IN ('Passed','Accepted','Completed') THEN 1 ELSE 0 END)/COUNT(*),2) AS pass_rate
    FROM pipeline_events pe JOIN candidates c ON pe.candidate_id=c.candidate_id
    GROUP BY c.level, pe.stage
""", conn)
exec_funnel["stage_order"] = exec_funnel["stage"].map({s:i for i,s in enumerate(STAGE_ORDER)})
exec_funnel = exec_funnel.sort_values("stage_order")

fig, ax = plt.subplots(figsize=(11, 5))
for lvl, grp in exec_funnel.groupby("level"):
    grp = grp[grp["stage"].isin(["CV Screening","Written Test","Interview",
                                   "Reference Check","Offer & Approval"])]
    ax.plot(grp["stage"], grp["pass_rate"], marker="o", linewidth=2.5,
            label=lvl, color=C[0] if lvl=="Non-Executive" else C[2])
ax.set_ylabel("Pass Rate (%)")
ax.set_title("Pass Rate by Stage: Executive vs Non-Executive", fontweight="bold", pad=12)
plt.xticks(rotation=20, ha="right")
ax.legend()
ax.set_ylim(0, 100)
save(fig, "05_exec_vs_nonexec_funnel.png")

# ── CHART 6: Top Rejection Reasons ───────────────────────────────────────────
rej = pd.read_sql("""
    SELECT outcome AS reason, COUNT(*) AS cnt
    FROM pipeline_events
    WHERE outcome NOT IN ('Passed','Accepted','Completed')
    GROUP BY outcome ORDER BY cnt DESC LIMIT 10
""", conn)

fig, ax = plt.subplots(figsize=(10, 5))
bars = ax.barh(rej["reason"], rej["cnt"],
               color=[C[2] if "Competitor" in r or "Declined" in r else C[0]
                      for r in rej["reason"]])
ax.bar_label(bars, padding=4)
ax.invert_yaxis()
ax.set_xlabel("Count")
ax.set_title("Top 10 Rejection & Dropout Reasons", fontweight="bold", pad=12)
save(fig, "06_rejection_reasons.png")

# ── CHART 7: Monthly Applications & Hires (2022 vs 2023) ─────────────────────
monthly = pd.read_sql("""
    SELECT strftime('%Y',applied_date) AS year,
           strftime('%m',applied_date) AS month,
           COUNT(*) AS applications, SUM(hired) AS hires
    FROM candidates GROUP BY year, month ORDER BY year, month
""", conn)
monthly["period"] = monthly["month"] + "/" + monthly["year"].str[-2:]

fig, ax1 = plt.subplots(figsize=(13, 5))
ax2 = ax1.twinx()
x = range(len(monthly))
ax1.bar(x, monthly["applications"], color=C[0], alpha=0.5, label="Applications")
ax2.plot(x, monthly["hires"], color=C[1], marker="o", linewidth=2.2, label="Hires")
ax1.set_xticks(x)
ax1.set_xticklabels(monthly["period"], rotation=45, ha="right", fontsize=8)
ax1.set_ylabel("Applications")
ax2.set_ylabel("Hires", color=C[1])
ax1.set_title("Monthly Applications & Hires — 2022 vs 2023", fontweight="bold", pad=12)
lines1, l1 = ax1.get_legend_handles_labels()
lines2, l2 = ax2.get_legend_handles_labels()
ax1.legend(lines1+lines2, l1+l2, loc="upper left")
# year divider
mid = len(monthly[monthly["year"]=="2022"])
ax1.axvline(mid - 0.5, color="grey", linestyle="--", linewidth=1)
ax1.text(mid/2, ax1.get_ylim()[1]*0.92, "2022", ha="center", color="grey", fontsize=10)
ax1.text(mid + (len(monthly)-mid)/2, ax1.get_ylim()[1]*0.92, "2023",
         ha="center", color="grey", fontsize=10)
save(fig, "07_monthly_hiring_trend.png")

conn.close()
print("\nAll 7 charts saved to visuals/")
