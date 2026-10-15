# SQL Sentinels - Healthcare Claims Edition

Autonomous regression testing for payer claims SQL on Databricks. Genie turns **business rules into validation SQL**; every prompt
comes from versioned **skill files** (`skills/*/SKILL.md`) and is logged to `meta.prompt_audit`.

## Run it in Databricks (about 10 minutes)
1. Import this folder as a **Git folder / Workspace folder** (keep `notebooks/`, `sentinel/`, `skills/`, `ddl/`, `sql/` together).
2. Open `notebooks/00_setup_ddl_and_data` -> Run all. Creates catalog `sentinel_hc`, DDL, ~20k synthetic claims, production silver + gold.
   The last cell prints the **Genie Space instructions** (rendered from `skills/genie-space-instructions`).
3. Create a Genie Space with the tables + instructions printed above; copy its Space ID.
4. Open `notebooks/01_run_sql_sentinels`, set widgets `genie_space_id`, `llm_endpoint` (an AI Gateway-governed endpoint) and
   `scenario` = `defects` | `intended` | `no_change`. Run all. Produces Trust Score, test table, and gold cascade check.
5. Open `notebooks/02_genie_insights` -> Genie answers the follow-up questions defined in `skills/genie-followup-questions`.
6. Optional: `sql/dashboard_queries.sql` -> AI/BI dashboard; `databricks bundle deploy` -> job + App; set `PGHOST` for Lakebase.

## Dataset (synthetic, no PHI) - DDL in `ddl/`
`raw.plan, member, member_eligibility, provider, ref_cpt, ref_icd10, claim_header, claim_line`  ->  `silver.claim_line_enriched`  ->  `gold.claims_kpi_monthly`.
Built-in traps: multi-span eligibility, claim adjustment versions, VOID/DENIED claims, out-of-network pricing.
CSV samples (1.5k claims) are in `data/`; the notebook generates the larger set with `sentinel/synth_claims.py`.

## Business SQL (`sql/business/`)
| File | Purpose |
|---|---|
| `silver_claim_line_v1.sql` | Baseline: latest version, eligibility-by-date, VOID filter, fee-capped allowed amount, cost share |
| `silver_claim_line_v2_defects.sql` | 3 injected defects: eligibility date dropped, VOID filter removed, fee cap ignored |
| `silver_claim_line_v3_intended.sql` | Intended additive change (adds `primary_dx`, `chronic_flag`) - should NOT be blocked |
| `gold_claims_kpi_monthly.sql` | Gold KPIs; `{{silver}}` swapped to test impact of a silver change |

## Skills (prompt library)
| Skill | Engine | Output |
|---|---|---|
| `domain-healthcare-claims` | included in all | glossary + rule catalog |
| `analyze-sql-change` | Genie, falls back to LLM | JSON: summary, rules, risks |
| `rule-to-validation-sql` | Genie, falls back to LLM | one SELECT returning `violations` |
| `edge-case-tests` | LLM | JSON tests |
| `classify-failures` | LLM | JSON labels |
| `genie-space-instructions`, `genie-followup-questions` | Genie | space setup / Q&A |
Edit a skill file and the behavior changes; no Python edits needed. `{{> name}}` includes other skills; `{{var}}` inputs are validated.

## What was verified
`python tests/test_local_duckdb.py` runs the full pipeline offline (DuckDB + a fake Genie/LLM that still renders the real skill files):
no-change -> SAFE (100), intended change -> SAFE (97), 3 defects -> BLOCK (0), gold cascade -> BLOCK.
**Not verified here:** live calls to Genie, the AI Gateway endpoint, Lakebase and Spark-specific behavior - validate those in your workspace.
Genie method names follow `databricks-sdk>=0.40`; adjust `sentinel/genie_client.py` if your SDK differs.
