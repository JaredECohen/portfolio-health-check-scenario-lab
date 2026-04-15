# QA Improvement Report

## Run Summary
- Total runs attempted: 100
- Total runs completed: 97
- Completion rate: 97.00%
- Success rate: 97.00%

## Score Distribution
- 0-1: 3
- 4-5: 97

## Best Question Types
- risk_adjusted_returns: 4.76/5
- rates_macro: 4.71/5
- geopolitical_war: 4.71/5
- macro_sensitivity: 4.71/5
- sector_diversification: 4.69/5

## Worst Question Types
- windowed: 2.33/5
- what_if_addition: 4.17/5
- earnings_overlay: 4.40/5
- drawdown_risk: 4.49/5
- general_health: 4.53/5

## Best Portfolio Archetypes
- cache_growth_barbell: 4.58/5
- cache_tech_core: 4.47/5

## Worst Portfolio Archetypes
- cache_tech_core: 4.47/5
- cache_growth_barbell: 4.58/5

## Top 10 Recurring Issues
- warnings_present: 97
- missing_candidate_search: 14
- planner_mismatch: 5
- technical_failure: 3

## Recommended Improvements
- P1: Harden candidate-search activation - Users asking what to add should consistently get screened candidates.
- P1: Tighten planner routing - Planner misclassifications reduce trust and lead to irrelevant workflows.
- P3: Classify and reduce degraded-mode runs - Frequent warnings indicate reliability or coverage gaps.

## Product / UX Improvements
- Surface the effective analysis window and degraded-mode warnings more prominently in the results header.
- Add question-specific empty-state guidance when candidate search or overlays are unavailable.
- Group warnings by source so users can tell whether the weakness came from data access, overlays, or planning.

## Analytics Improvements
- Expand candidate ranking diagnostics so users can see why top candidates were selected or rejected.
- Add stronger consistency checks between requested window and effective aligned sample for every overlay and table.

## Agent / Prompt Improvements
- Tighten planner instructions for ambiguous user phrasing and benchmark-underperformance prompts.
- Add explicit guardrails so writer/critic always reference quantitative evidence in the memo body.
- Capture planner misroutes in telemetry and retrain prompt examples around those exact phrasings.

## Reliability / Test Improvements
- Persist run-level request IDs and backend warning categories for easier debugging.
- Add nightly regression runs over a smaller fixed portfolio/question suite.
- Cache more market-data and overlay inputs to reduce overnight evaluation cost and rate-limit exposure.
- Separate overlay failures from core analytics failures in structured response telemetry.

## Evaluation Framework Improvements
- Add a small human-reviewed gold set to calibrate the deterministic rubric.
- Track longitudinal output drift by saving planner type, workflow, warnings, and memo metrics per run.

## Notes
- Runs are checkpointed in results.jsonl and can be resumed.
- Scoring is deterministic and rubric-based; it is designed to highlight likely quality gaps rather than replace human review.