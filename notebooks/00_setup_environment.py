# Databricks notebook source
# MAGIC %md
# MAGIC # 00 — Setup Environment
# MAGIC
# MAGIC **AI-Powered ZeroOps for Databricks ETL Pipelines**
# MAGIC
# MAGIC ### ⚠️ Community Edition note
# MAGIC Databricks Community Edition (CE) clusters have **no outbound internet access**.
# MAGIC That means any step that would normally call an external API — OpenAI/Claude for
# MAGIC root-cause analysis, the GitHub REST API to open a PR, or a Slack/Teams webhook for
# MAGIC notifications — cannot reach the internet from this cluster.
# MAGIC
# MAGIC This project still implements the **full ZeroOps architecture**, but the
# MAGIC internet-dependent steps run in **simulation mode**:
# MAGIC - Root cause / fix generation uses a deterministic **rule engine** instead of an LLM call
# MAGIC - "GitHub PR creation" does a real local `git init/commit/diff` and produces a PR-ready
# MAGIC   patch + description, but does not call the GitHub API
# MAGIC - "Notifications" are written to a Delta table and printed, instead of POSTed to Slack/Teams
# MAGIC
# MAGIC Every notebook that stands in for a real API call includes a **commented-out block**
# MAGIC showing exactly what to uncomment when you move this to an internet-enabled workspace
# MAGIC (your own laptop, a paid Databricks workspace, or a GitHub Actions runner).
# MAGIC
# MAGIC ### Catalog layout
# MAGIC Everything lives under one Unity Catalog (`catalog_name` widget, default `zeroops`),
# MAGIC split into five schemas: `bronze`, `silver`, `gold` for the medallion data layers,
# MAGIC `ops` for ZeroOps metadata (incidents, AI analysis/fix, validation results, PR
# MAGIC history, notifications, guardrail log), and `sandbox` for notebook 07's per-incident
# MAGIC validation clones.
# MAGIC
# MAGIC ### File storage: Unity Catalog Volumes, not DBFS
# MAGIC Community Edition workspaces increasingly restrict direct `dbfs:/` root access.
# MAGIC This project stores every file (input CSVs, the simulated git repo, generated fix
# MAGIC code, PR artifacts) in two **Unity Catalog Volumes** instead:
# MAGIC - `{catalog}.bronze.landing` — input CSVs (upload `data/*.csv` here)
# MAGIC - `{catalog}.ops.artifacts` — simulated repo, generated fixes, PR diffs/descriptions
# MAGIC
# MAGIC Both are created automatically below. Volume paths look like
# MAGIC `/Volumes/{catalog}/{schema}/{volume}/...` and are read/written directly with plain
# MAGIC Python `open()` or `dbutils.fs` — no `/dbfs` prefix needed.

# COMMAND ----------

dbutils.widgets.text("catalog_name", "zeroops", "Unity Catalog name")
CATALOG = dbutils.widgets.get("catalog_name")

# COMMAND ----------

import json
import uuid
import datetime
from pyspark.sql import functions as F
from pyspark.sql.types import *

# ---------------------------------------------------------------------------
# CONFIG — mirrors config/settings.json shipped alongside the notebooks folder.
# Kept as a plain dict here (rather than reading the JSON file) so every
# notebook works standalone the moment it's imported into a fresh workspace,
# with no dependency on file upload order.
#
# Layout: one Unity Catalog (default name "zeroops") with five schemas:
#   bronze  -- raw ingested data
#   silver  -- cleaned/joined/deduped data (+ quarantine, + unmapped columns)
#   gold    -- business aggregations
#   ops     -- ZeroOps metadata: incidents, AI analysis/fix, validation results,
#              PR history, notifications, guardrail audit log. These aren't part
#              of the medallion data layers, so they get their own schema rather
#              than being folded into bronze/silver/gold.
#   sandbox -- per-incident validation clones created at runtime by notebook 07
# ---------------------------------------------------------------------------
CONFIG = {
    "catalog": CATALOG,
    "schemas": {
        "bronze": "bronze",
        "silver": "silver",
        "gold": "gold",
        "ops": "ops",
        "sandbox": "sandbox",
    },
    "tables": {
        "bronze": f"{CATALOG}.bronze.sales",
        "silver": f"{CATALOG}.silver.sales",
        "silver_quarantine": f"{CATALOG}.silver.sales_quarantine",
        "silver_unmapped_columns": f"{CATALOG}.silver.unmapped_columns",
        "gold_daily": f"{CATALOG}.gold.daily_sales",
        "gold_monthly": f"{CATALOG}.gold.monthly_revenue",
        "run_history": f"{CATALOG}.ops.run_history",
        "incident_log": f"{CATALOG}.ops.incident_log",
        "ai_analysis": f"{CATALOG}.ops.ai_analysis",
        "ai_fix": f"{CATALOG}.ops.ai_fix",
        "validation_results": f"{CATALOG}.ops.validation_results",
        "github_pr_history": f"{CATALOG}.ops.github_pr_history",
        "notifications": f"{CATALOG}.ops.notifications",
        "guardrail_log": f"{CATALOG}.ops.guardrail_log",
    },
    "sandbox_schema": f"{CATALOG}.sandbox",
    "volumes": {
        # schema -> volume name. Both created by ensure_catalog_and_schemas().
        "bronze": "landing",
        "ops": "artifacts",
    },
    "paths": {
        "data": f"/Volumes/{CATALOG}/bronze/landing",
        "repo_sim": f"/Volumes/{CATALOG}/ops/artifacts/repo_sim/pipeline-repo",
        "generated_fixes": f"/Volumes/{CATALOG}/ops/artifacts/generated_fixes",
        "prompts": f"/Volumes/{CATALOG}/ops/artifacts/prompts",
        "pr_artifacts": f"/Volumes/{CATALOG}/ops/artifacts/pr_artifacts",
    },
    "expected_bronze_schema": [
        "order_id", "order_date", "store_id", "product_id",
        "product_name", "quantity", "unit_price", "customer_id",
    ],
    "thresholds": {
        "row_count_change_pct": 0.30,   # flag if row count changes by more than 30% run-over-run
        "null_pct": 0.03,               # flag if a key column has more than 3% nulls
        "duplicate_pct": 0.02,          # flag if more than 2% of order_ids are duplicated
    },
    "llm": {
        # Databricks Free Edition (formerly Community Edition) ships a system.ai catalog of
        # pre-configured, pay-per-token model services available to every account for free
        # (fair-use quotas apply). Query them via this workspace's OWN url + the notebook's
        # own token -- no secret scope, no external internet call, no API key.
        "databricks_model": "databricks-claude-sonnet-4",
        # Real OpenAI, opt-in only: needs actual outbound internet + a paid API key stored
        # in a secret scope (secret scopes require a non-Free-Edition workspace).
        "openai_model": "gpt-4o-mini",
    },
    "policies": {
        # Below this confidence, a fix can still be generated and validated, but the PR
        # is flagged NEEDS_HUMAN_REVIEW instead of routed for normal review.
        "min_confidence_for_auto_pr": 0.70,
        # Any table an AI-generated fix writes to must live under this catalog. Enforced
        # by scanning the generated code text, independent of what apply_fix() actually
        # runs (defense in depth: the vetted function is already scoped, this catches the
        # case where the raw generated code disagrees with what got executed).
        "allowed_table_prefix": f"{CATALOG}.",
        # Patterns that block a generated fix outright -- it's written to ai_fix with
        # guardrail_status=BLOCKED and never reaches the DBFS file / git commit step.
        "dangerous_code_patterns": [
            r"\bdrop\s+table\b", r"\bdrop\s+database\b", r"\bdrop\s+catalog\b", r"\btruncate\b",
            r"\bos\.system\b", r"\bsubprocess\.", r"\brm\s+-rf\b",
            r"dbutils\.secrets", r"\bexec\(", r"\beval\(",
            r"\bgrant\s+all\b", r"\.credentials\b",
        ],
        # Columns treated as potentially sensitive; redacted out of any text sent to an
        # LLM prompt (data-minimization guardrail).
        "pii_like_columns": ["customer_id"],
    },
}

# COMMAND ----------

def log(msg: str) -> None:
    """Lightweight structured print-logging (stands in for a real logging/observability
    sink such as Datadog or Azure Monitor in a production deployment)."""
    ts = datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{ts} UTC] {msg}")


def new_id() -> str:
    return str(uuid.uuid4())


def ensure_catalog_and_schemas() -> None:
    try:
        spark.sql(f"CREATE CATALOG IF NOT EXISTS {CONFIG['catalog']}")
    except Exception as e:
        log(f"WARNING: could not create catalog '{CONFIG['catalog']}' ({e}). "
            f"If your account lacks catalog-creation privilege, ask a workspace admin, "
            f"or set the 'catalog_name' widget to an existing catalog you already have "
            f"CREATE SCHEMA rights on.")

    for schema in CONFIG["schemas"].values():
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CONFIG['catalog']}.{schema}")

    for schema, volume in CONFIG["volumes"].items():
        spark.sql(f"CREATE VOLUME IF NOT EXISTS {CONFIG['catalog']}.{schema}.{volume}")

    log(f"Catalog '{CONFIG['catalog']}' ready with schemas: "
        f"{', '.join(CONFIG['schemas'].values())} and volumes: "
        f"{', '.join(f'{s}.{v}' for s, v in CONFIG['volumes'].items())}")


def grant_volume_read_access(principal: str) -> None:
    """Optional: grant another user/group read access to the input-data volume.
    Not needed on a single-user Free Edition workspace (you own everything you
    create), but useful the moment this project is shared with a team or run in
    a paid workspace. See ddl/create_zeroops_schema.sql for the SQL equivalent."""
    catalog, schema, volume = CONFIG["catalog"], "bronze", CONFIG["volumes"]["bronze"]
    spark.sql(f"GRANT USE CATALOG ON CATALOG {catalog} TO `{principal}`")
    spark.sql(f"GRANT USE SCHEMA ON SCHEMA {catalog}.{schema} TO `{principal}`")
    spark.sql(f"GRANT READ VOLUME ON VOLUME {catalog}.{schema}.{volume} TO `{principal}`")
    log(f"Granted READ VOLUME on {catalog}.{schema}.{volume} to {principal}.")


def ensure_dirs() -> None:
    for p in CONFIG["paths"].values():
        dbutils.fs.mkdirs(p)
    log("DBFS working directories ready: " + ", ".join(CONFIG["paths"].values()))


def table_exists(full_name: str) -> bool:
    """full_name is a 3-level catalog.schema.table name."""
    try:
        return spark.catalog.tableExists(full_name)
    except Exception:
        return False

# COMMAND ----------

# ---------------------------------------------------------------------------
# LLM helpers
# ---------------------------------------------------------------------------

def get_workspace_url() -> str:
    return "https://" + spark.conf.get("spark.databricks.workspaceUrl")


def get_notebook_token() -> str:
    ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
    return ctx.apiToken().get()


def query_databricks_llm(prompt: str, system: str = None,
                          model: str = None, max_tokens: int = 800) -> str:
    """Query a Databricks-hosted Foundation Model (system.ai catalog, pay-per-token,
    free-tier eligible under fair-use quotas) directly from this notebook.

    This works without any secret scope because it authenticates with the notebook's
    own built-in token, and it doesn't require general internet access because it
    calls THIS workspace's own url (/serving-endpoints/...), not an external domain.
    """
    import requests
    model = model or CONFIG["llm"]["databricks_model"]
    url = f"{get_workspace_url()}/serving-endpoints/{model}/invocations"
    token = get_notebook_token()

    messages = []
    if system:
        messages.append({"role": "system", "content": system})
    messages.append({"role": "user", "content": prompt})

    resp = requests.post(
        url,
        headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
        json={"messages": messages, "max_tokens": max_tokens},
        timeout=60,
    )
    resp.raise_for_status()
    return resp.json()["choices"][0]["message"]["content"]


def query_openai_llm(prompt: str, system: str = None, model: str = None,
                      max_tokens: int = 800) -> str:
    """Query the real OpenAI API. Opt-in only: requires actual outbound internet
    access (not guaranteed on every Databricks tier) and a paid API key. Reads the
    key from a secret scope if available, else from an environment variable."""
    import requests
    model = model or CONFIG["llm"]["openai_model"]
    try:
        api_key = dbutils.secrets.get("zeroops", "openai_api_key")
    except Exception:
        import os
        api_key = os.environ.get("OPENAI_API_KEY")
    if not api_key:
        raise RuntimeError(
            "No OpenAI API key found. Set secret scope 'zeroops'/'openai_api_key' "
            "or the OPENAI_API_KEY environment variable."
        )

    messages = []
    if system:
        messages.append({"role": "system", "content": system})
    messages.append({"role": "user", "content": prompt})

    resp = requests.post(
        "https://api.openai.com/v1/chat/completions",
        headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
        json={"model": model, "messages": messages, "max_tokens": max_tokens},
        timeout=60,
    )
    resp.raise_for_status()
    return resp.json()["choices"][0]["message"]["content"]


def query_llm(prompt: str, backend: str, system: str = None) -> str:
    """Single entry point used by notebooks 05/06/07. backend is one of
    'databricks_llm', 'openai', or 'rule_engine' (caller handles rule_engine itself,
    this function is only invoked for the two LLM-backed options)."""
    if backend == "databricks_llm":
        return query_databricks_llm(prompt, system=system)
    elif backend == "openai":
        return query_openai_llm(prompt, system=system)
    else:
        raise ValueError(f"query_llm() does not handle backend='{backend}'")


def strip_code_fences(text: str) -> str:
    """LLMs sometimes wrap code in ``` fences despite instructions not to. Strip them."""
    t = text.strip()
    if t.startswith("```"):
        lines = t.split("\n")
        lines = lines[1:]
        if lines and lines[-1].strip().startswith("```"):
            lines = lines[:-1]
        t = "\n".join(lines)
    return t.strip()


def parse_json_loose(text: str) -> dict:
    """Parse a JSON object out of an LLM response, tolerating stray text/fences."""
    t = strip_code_fences(text)
    start = t.find("{")
    end = t.rfind("}")
    if start == -1 or end == -1:
        raise ValueError(f"No JSON object found in LLM response: {text[:200]}")
    return json.loads(t[start:end + 1])

# COMMAND ----------

# ---------------------------------------------------------------------------
# Guardrails / policy enforcement
#
# These are the actual enforcement points, not just documentation: 06 calls
# scan_dangerous_patterns()/check_table_scope() and BLOCKS a fix if either fires; 05
# calls confidence_gate_passed() to decide whether a PR can auto-proceed or must be
# flagged for human review; every decision is written to zeroops.guardrail_log for audit.
# ---------------------------------------------------------------------------

import re as _re


def log_guardrail_decision(policy: str, subject_id: str, stage: str,
                            decision: str, reason: str) -> None:
    """Append an audit row. Called on every guardrail check, pass or fail, so the
    log is a complete record of what was allowed and what was blocked, not just
    the failures."""
    row = {
        "policy": policy,
        "subject_id": subject_id,
        "stage": stage,
        "decision": decision,   # "ALLOW" or "BLOCK"
        "reason": reason,
        "logged_at": datetime.datetime.utcnow(),
    }
    spark.createDataFrame([row]).write.format("delta").mode("append") \
        .option("mergeSchema", "true").saveAsTable(CONFIG["tables"]["guardrail_log"])


def redact_pii(text: str) -> str:
    """Data-minimization guardrail: strip values of configured PII-like columns out
    of any text before it's sent to an LLM prompt."""
    redacted = text
    for col in CONFIG["policies"]["pii_like_columns"]:
        redacted = _re.sub(
            rf'({col}\s*[=:]\s*)["\']?[\w\-]+["\']?',
            rf'\1"[REDACTED]"',
            redacted,
            flags=_re.IGNORECASE,
        )
    return redacted


def scan_dangerous_patterns(code: str) -> list:
    """Safety guardrail: returns the list of banned patterns found in generated code.
    Empty list means the code passed this check."""
    hits = []
    for pattern in CONFIG["policies"]["dangerous_code_patterns"]:
        if _re.search(pattern, code, _re.IGNORECASE):
            hits.append(pattern)
    return hits


def check_table_scope(code: str) -> list:
    """Policy enforcement: returns any table name written in generated code that
    falls outside the allowed prefix. Empty list means fully in-scope."""
    violations = []
    for m in _re.finditer(r'saveAsTable\(\s*["\']([^"\']+)["\']', code):
        table = m.group(1)
        if not table.startswith(CONFIG["policies"]["allowed_table_prefix"]):
            violations.append(table)
    return violations


def confidence_gate_passed(confidence: float) -> bool:
    """Policy: only confidence at or above the configured threshold may proceed to
    an automatic PR without a human-review flag."""
    return confidence >= CONFIG["policies"]["min_confidence_for_auto_pr"]


def enforce_fix_guardrails(incident_id: str, generated_code: str) -> dict:
    """Runs every code-level guardrail against a generated fix, logs each decision,
    and returns a single verdict dict used by notebook 06 to decide whether the fix
    is written to disk at all."""
    dangerous_hits = scan_dangerous_patterns(generated_code)
    scope_violations = check_table_scope(generated_code)

    if dangerous_hits:
        log_guardrail_decision(
            "dangerous_code_pattern", incident_id, "fix_generation", "BLOCK",
            f"Matched banned pattern(s): {dangerous_hits}",
        )
    else:
        log_guardrail_decision(
            "dangerous_code_pattern", incident_id, "fix_generation", "ALLOW", "No banned patterns matched.",
        )

    if scope_violations:
        log_guardrail_decision(
            "table_scope", incident_id, "fix_generation", "BLOCK",
            f"Fix writes to out-of-scope table(s): {scope_violations}",
        )
    else:
        log_guardrail_decision(
            "table_scope", incident_id, "fix_generation", "ALLOW",
            f"All writes stay under '{CONFIG['policies']['allowed_table_prefix']}'.",
        )

    blocked = bool(dangerous_hits or scope_violations)
    return {
        "status": "BLOCKED" if blocked else "PASSED",
        "dangerous_hits": dangerous_hits,
        "scope_violations": scope_violations,
    }

# COMMAND ----------

ensure_catalog_and_schemas()
ensure_dirs()
log("Environment setup complete. CONFIG is available to any notebook that runs "
    "`%run ./00_setup_environment` first.")
