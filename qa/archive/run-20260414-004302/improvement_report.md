# QA Improvement Report

## Run Summary
- Total runs attempted: 100
- Total runs completed: 94
- Completion rate: 94.00%
- Success rate: 94.00%

## Score Distribution
- 0-1: 6
- 3-4: 2
- 4-5: 92

## Best Question Types
- sector_diversification: 4.83/5
- concentration_diversification: 4.81/5
- macro_sensitivity: 4.79/5
- general_health: 4.74/5
- benchmark_underperformance: 4.74/5

## Worst Question Types
- windowed: 0.00/5
- risk_adjusted_returns: 4.19/5
- rates_macro: 4.19/5
- earnings_overlay: 4.21/5
- what_if_addition: 4.30/5

## Best Portfolio Archetypes
- cache_tech_core: 4.72/5
- cache_ten_name: 4.72/5
- cache_balanced: 4.70/5
- cache_high_beta: 4.69/5
- cache_barbell: 4.67/5

## Worst Portfolio Archetypes
- cache_energy_finance: 2.76/5
- cache_five_name: 3.63/5
- cache_defensive_growth: 3.70/5
- cache_drawdown_test: 3.70/5
- cache_energy_hedge: 3.71/5

## Top 10 Recurring Issues
- sparse_eda: 20
- planner_mismatch: 15
- technical_failure: 6
- missing_candidate_search: 4
- warnings_present: 1

## Recommended Improvements
- P1: Increase question-specific EDA depth - Thin EDA makes memos feel generic.
- P1: Tighten planner routing - Planner misclassifications reduce trust and lead to irrelevant workflows.
- P1: Harden candidate-search activation - Users asking what to add should consistently get screened candidates.
- P3: Classify and reduce degraded-mode runs - Frequent warnings indicate reliability or coverage gaps.

## Product / UX Improvements
- Surface the effective analysis window and degraded-mode warnings more prominently in the results header.
- Add question-specific empty-state guidance when candidate search or overlays are unavailable.
- Group warnings by source so users can tell whether the weakness came from data access, overlays, or planning.

## Analytics Improvements
- Expand candidate ranking diagnostics so users can see why top candidates were selected or rejected.
- Add stronger consistency checks between requested window and effective aligned sample for every overlay and table.
- Add more question-type-specific tables so different prompts visibly trigger different investigations.

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