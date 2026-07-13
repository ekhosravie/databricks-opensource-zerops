# Databricks notebook source
# MAGIC %md
# MAGIC # 08 — Create GitHub PR (simulated)
# MAGIC
# MAGIC Production version: pushes a branch and calls the GitHub REST API to open a real
# MAGIC pull request. **The GitHub API needs outbound internet, which Community Edition
# MAGIC blocks**, so this notebook does everything that doesn't require the network:
# MAGIC - a real local `git init` / branch / commit against a small simulated repo in a
# MAGIC   Unity Catalog Volume (`{catalog}.ops.artifacts`)
# MAGIC - a real `git diff` between `main` and the fix branch
# MAGIC - a PR description file, ready to paste into GitHub the moment you have internet
# MAGIC
# MAGIC It stops one step short of `git push` + the `POST /pulls` call, and prints exactly
# MAGIC what to run to finish the job from an internet-enabled machine.

# COMMAND ----------

# MAGIC %run ./00_setup_environment

# COMMAND ----------

import os

REPO_DIR = CONFIG["paths"]["repo_sim"]
dbutils.fs.mkdirs(CONFIG["paths"]["repo_sim"])
dbutils.fs.mkdirs(CONFIG["paths"]["pr_artifacts"])

# %sh cells run as a subprocess of this same notebook's driver process, so an
# env var set here is visible to the %sh cell below -- this is how a Volume
# path (built dynamically from the catalog_name widget) reaches a shell script
# without hardcoding it.
os.environ["ZEROOPS_REPO_DIR"] = REPO_DIR

# COMMAND ----------

# MAGIC %sh
# MAGIC set -e
# MAGIC REPO_DIR="$ZEROOPS_REPO_DIR"
# MAGIC if [ ! -d "$REPO_DIR/.git" ]; then
# MAGIC   mkdir -p "$REPO_DIR"
# MAGIC   cd "$REPO_DIR"
# MAGIC   git init -q
# MAGIC   git config user.email "zeroops-bot@example.com"
# MAGIC   git config user.name "ZeroOps Bot"
# MAGIC   mkdir -p pipelines
# MAGIC   cat > pipelines/silver_transformation.py << 'PYEOF'
# MAGIC # Baseline Silver transformation (mirrors notebooks/02_silver_transformation.py)
# MAGIC casted_df = bronze_df.withColumn("quantity", col("quantity").cast("int"))
# MAGIC selected_df = casted_df.select(
# MAGIC     "order_id", "order_date", "store_id", "product_id",
# MAGIC     "product_name", "quantity", "unit_price", "customer_id",
# MAGIC )
# MAGIC PYEOF
# MAGIC   git add -A
# MAGIC   git commit -q -m "Initial pipeline baseline"
# MAGIC   git branch -M main
# MAGIC   echo "Initialized simulated repo at $REPO_DIR"
# MAGIC else
# MAGIC   echo "Simulated repo already exists at $REPO_DIR"
# MAGIC fi

# COMMAND ----------

fixes_to_pr = (
    spark.table(CONFIG["tables"]["validation_results"])
    .filter(F.col("status") == "VALIDATION_PASSED")
    .join(spark.table(CONFIG["tables"]["ai_fix"]), on=["incident_id", "error_type"])
    .join(
        spark.table(CONFIG["tables"]["ai_analysis"])
        .select("incident_id", "root_cause", "business_impact", "confidence", "requires_human_review"),
        on="incident_id",
    )
    .collect()
)

already_pr = set()
if table_exists(CONFIG["tables"]["github_pr_history"]):
    already_pr = {
        r.incident_id for r in spark.table(CONFIG["tables"]["github_pr_history"])
        .select("incident_id").collect()
    }

pr_rows = []
for row in fixes_to_pr:
    d = row.asDict()
    if d["incident_id"] in already_pr:
        continue

    short_id = d["incident_id"][:8]
    branch_name = f"fix/{d['error_type'].lower()}-{short_id}"
    fix_filename = f"pipelines/fix_{short_id}.py"

    diff_path = f"{CONFIG['paths']['pr_artifacts']}/pr_artifacts_{short_id}.diff"
    pr_desc_path = f"{CONFIG['paths']['pr_artifacts']}/pr_description_{short_id}.md"

    # Policy enforcement: every PR requires human approval to merge regardless (this
    # notebook never pushes/merges on its own) -- but a low-confidence analysis gets an
    # extra, loud banner so a reviewer doesn't treat it as routine.
    review_banner = (
        "\n> ⚠️ **NEEDS HUMAN REVIEW** — AI confidence "
        f"({d['confidence']:.0%}) is below the "
        f"{CONFIG['policies']['min_confidence_for_auto_pr']:.0%} auto-PR threshold. "
        "Do not merge without independent verification of the root cause.\n"
        if d["requires_human_review"] else ""
    )

    # Separate from the confidence gate: this fix may be mechanically clean and still
    # have semantically drifted from the metric contract (e.g. quietly redefining
    # what counts toward revenue). Flagged independently because a fix can pass one
    # check and fail the other.
    semantic_banner = (
        "\n> 🧭 **NEEDS SEMANTIC REVIEW** — this fix's business logic has drifted "
        f"from the expected metric definition (similarity={d['semantic_similarity']}, "
        f"threshold={CONFIG['policies']['min_semantic_similarity']}). Mechanical "
        "checks passed, but verify with the metric owner that this doesn't change "
        "what a downstream number means before merging.\n"
        if d.get("requires_semantic_review") else ""
    )

    # write PR description now (bash cell below does the actual git work)
    pr_description = f"""# Fix: {d['error_type']} (incident {short_id})
{review_banner}{semantic_banner}
## Root cause
{d['root_cause']}

## Business impact
{d['business_impact']}

## Fix applied
{d['fix_summary']}

## Validation
Sandbox validation status: **{d['status']}**
Semantic similarity to metric contract: **{d['semantic_similarity']}**
See `{CONFIG['tables']['validation_results']}` for before/after metrics.

## Generated by
{d['generated_by']}
"""
    with open(pr_desc_path, "w") as f:
        f.write(pr_description)

    log_guardrail_decision(
        "human_approval_required", d["incident_id"], "pr_creation",
        "BLOCK" if d["requires_human_review"] else "ALLOW",
        f"requires_human_review={d['requires_human_review']} (confidence={d['confidence']:.2f})",
    )

    pr_rows.append({
        "incident_id": d["incident_id"],
        "error_type": d["error_type"],
        "branch_name": branch_name,
        "fix_filename": fix_filename,
        "pr_description_path": pr_desc_path,
        "requires_human_review": d["requires_human_review"],
        "requires_semantic_review": bool(d.get("requires_semantic_review")),
        "status": "SIMULATED_PR_READY",
        "created_at": datetime.datetime.utcnow(),
    })

log(f"Prepared {len(pr_rows)} simulated PR(s) "
    f"({sum(1 for r in pr_rows if r['requires_human_review'])} flagged for human review, "
    f"{sum(1 for r in pr_rows if r['requires_semantic_review'])} flagged for semantic review).")

# COMMAND ----------

for row in pr_rows:
    short_id = row["incident_id"][:8]
    fix_src = f"{CONFIG['paths']['generated_fixes']}/fix_{row['incident_id']}.py"
    dst = f"{REPO_DIR}/{row['fix_filename']}"

    branch = row["branch_name"]
    import subprocess
    subprocess.run(["git", "-C", REPO_DIR, "checkout", "main"], check=True)
    subprocess.run(["git", "-C", REPO_DIR, "checkout", "-B", branch], check=True)

    with open(fix_src) as f_in, open(dst, "w") as f_out:
        f_out.write(f_in.read())

    subprocess.run(["git", "-C", REPO_DIR, "add", "-A"], check=True)
    subprocess.run(
        ["git", "-C", REPO_DIR, "commit", "-q", "-m",
         f"Fix {row['error_type']} (incident {short_id})"],
        check=True,
    )

    diff = subprocess.run(
        ["git", "-C", REPO_DIR, "diff", "main", branch],
        check=True, capture_output=True, text=True,
    ).stdout
    diff_path = f"{CONFIG['paths']['pr_artifacts']}/pr_artifacts_{short_id}.diff"
    with open(diff_path, "w") as f:
        f.write(diff)

    log(f"Committed fix to local branch '{branch}'. Diff written to {diff_path}")

# COMMAND ----------

if pr_rows:
    pr_df = spark.createDataFrame(pr_rows)
    pr_df.write.format("delta").mode("append").option("mergeSchema", "true") \
        .saveAsTable(CONFIG["tables"]["github_pr_history"])
    display(pr_df)

    for row in pr_rows:
        log(
            "\nTo finish this PR from a machine with internet access:\n"
            f"  git push origin {row['branch_name']}\n"
            "  curl -X POST https://api.github.com/repos/<owner>/<repo>/pulls \\\n"
            "       -H \"Authorization: token $GITHUB_TOKEN\" \\\n"
            "       -d '{\"title\": \"Fix " + row["error_type"] + "\", "
            f"\"head\": \"{row['branch_name']}\", \"base\": \"main\", "
            f"\"body\": \"<paste {row['pr_description_path']}>\"}}'"
        )

# COMMAND ----------

# MAGIC %md
# MAGIC ### Production reference (commented out — requires internet)

# COMMAND ----------

# import requests
#
# def open_github_pr(owner: str, repo: str, branch: str, pr_title: str, pr_body: str, token: str) -> dict:
#     response = requests.post(
#         f"https://api.github.com/repos/{owner}/{repo}/pulls",
#         headers={"Authorization": f"token {token}", "Accept": "application/vnd.github+json"},
#         json={"title": pr_title, "head": branch, "base": "main", "body": pr_body},
#         timeout=30,
#     )
#     response.raise_for_status()
#     return response.json()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Notes
# MAGIC - **Guardrails applied here:** only incidents whose fix has `guardrail_status =
# MAGIC   PASSED` and whose validation is `VALIDATION_PASSED` ever reach this notebook —
# MAGIC   both are enforced upstream (06 and 07), not re-checked here, so there's a single
# MAGIC   source of truth for each gate.
# MAGIC - **Policy enforcement:** every PR requires a human to actually push/merge it
# MAGIC   regardless of confidence (this notebook only prepares a local branch + diff).
# MAGIC   Low-confidence incidents (`requires_human_review = True`, set in notebook 05)
# MAGIC   get an extra ⚠️ banner at the top of the PR description so a reviewer treats it
# MAGIC   with appropriately less trust than a high-confidence one. The decision is logged
# MAGIC   to `zeroops.guardrail_log` under policy `human_approval_required`.
# MAGIC - **Semantic drift, separately from confidence:** a fix can be mechanically clean
# MAGIC   (passes 07's validation) and still `requires_semantic_review = True` if its
# MAGIC   embedding drifted from `CANONICAL_METRIC_CONTRACT` (07). That gets its own 🧭
# MAGIC   banner, independent of the confidence-based ⚠️ one, because a fix can pass one
# MAGIC   of these checks and fail the other.
