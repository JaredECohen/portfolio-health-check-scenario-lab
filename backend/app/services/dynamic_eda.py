from __future__ import annotations

import numpy as np
import pandas as pd

from app.models.schemas import AnalysisPlan, AnalysisTable, DynamicEDAResult, EDAFinding, QuestionType
from app.services.analytics import AnalyticsBundle
from app.services.alpha_vantage import AlphaVantageService


class DynamicEDAService:
    def __init__(self, alpha_vantage: AlphaVantageService) -> None:
        self.alpha_vantage = alpha_vantage

    async def execute(
        self,
        *,
        plan: AnalysisPlan,
        question: str,
        baseline_bundle: AnalyticsBundle,
    ) -> DynamicEDAResult:
        if plan.question_type == QuestionType.general_health:
            return self._general_health(plan, baseline_bundle)
        if plan.question_type == QuestionType.concentration_diversification:
            return self._concentration(plan, baseline_bundle)
        if plan.question_type == QuestionType.performance_drivers:
            return self._performance(plan, baseline_bundle)
        if plan.question_type == QuestionType.rates_macro:
            return await self._rates(plan, baseline_bundle)
        if plan.question_type == QuestionType.geopolitical_war:
            return await self._war(plan, baseline_bundle)
        return self._what_if(plan, baseline_bundle, question)

    def _general_health(
        self,
        plan: AnalysisPlan,
        baseline_bundle: AnalyticsBundle,
    ) -> DynamicEDAResult:
        top_sector = baseline_bundle.baseline.sector_exposures[0]
        findings = [
            EDAFinding(
                headline="Concentration and volatility are the first-order health signals.",
                evidence=[
                    f"Top 3 holdings account for {baseline_bundle.metrics_map['top3_share'] * 100:.2f}% of capital.",
                    f"Herfindahl concentration index is {baseline_bundle.metrics_map['herfindahl_index']:.2f}.",
                    f"Annualized volatility is {baseline_bundle.metrics_map['annualized_volatility'] * 100:.2f}%.",
                ],
                metrics={
                    "top3_share": baseline_bundle.metrics_map["top3_share"],
                    "herfindahl_index": baseline_bundle.metrics_map["herfindahl_index"],
                    "annualized_volatility": baseline_bundle.metrics_map["annualized_volatility"],
                },
            ),
            EDAFinding(
                headline="Sector skew is visible in the baseline exposure table.",
                evidence=[
                    f"The largest sector is {top_sector.sector} at {top_sector.weight * 100:.2f}% weight.",
                    f"Average pairwise correlation across holdings is {baseline_bundle.metrics_map['average_pairwise_correlation']:.2f}.",
                    f"Beta vs benchmark is {baseline_bundle.metrics_map['beta_vs_benchmark']:.2f}.",
                ],
                metrics={
                    "largest_sector_weight": top_sector.weight,
                    "average_pairwise_correlation": baseline_bundle.metrics_map["average_pairwise_correlation"],
                    "beta_vs_benchmark": baseline_bundle.metrics_map["beta_vs_benchmark"],
                },
            ),
        ]
        return DynamicEDAResult(
            workflow=plan.dynamic_workflow,
            question_type=plan.question_type,
            findings=findings,
            tables=[
                AnalysisTable(
                    name="Top Contributors",
                    columns=["ticker", "contribution_pct", "weight", "return_pct"],
                    rows=[item.model_dump() for item in baseline_bundle.baseline.contributors[:5]],
                )
            ],
        )

    def _concentration(
        self,
        plan: AnalysisPlan,
        baseline_bundle: AnalyticsBundle,
    ) -> DynamicEDAResult:
        correlation_pairs = []
        matrix = baseline_bundle.baseline.correlation_matrix
        tickers = list(matrix.keys())
        for index, ticker in enumerate(tickers):
            for other in tickers[index + 1 :]:
                correlation_pairs.append((ticker, other, matrix[ticker][other]))
        correlation_pairs.sort(key=lambda item: item[2], reverse=True)
        top_pair = correlation_pairs[0] if correlation_pairs else ("", "", 0.0)
        findings = [
            EDAFinding(
                headline="The portfolio's diversification constraint is concentrated in a few names.",
                evidence=[
                    f"Top 3 holdings account for {baseline_bundle.metrics_map['top3_share'] * 100:.2f}% of value.",
                    f"Average pairwise correlation is {baseline_bundle.metrics_map['average_pairwise_correlation']:.2f}.",
                    f"Most correlated pair is {top_pair[0]} / {top_pair[1]} at {top_pair[2]:.2f}.",
                ],
                metrics={
                    "top3_share": baseline_bundle.metrics_map["top3_share"],
                    "average_pairwise_correlation": baseline_bundle.metrics_map["average_pairwise_correlation"],
                    "top_pair_correlation": top_pair[2],
                },
            ),
            EDAFinding(
                headline="Sector concentration adds a second layer of clustering.",
                evidence=[
                    f"Largest sector weight is {baseline_bundle.baseline.sector_exposures[0].weight * 100:.2f}%.",
                    f"Herfindahl index is {baseline_bundle.metrics_map['herfindahl_index']:.2f}.",
                    f"Sharpe is {baseline_bundle.metrics_map['sharpe_ratio']:.2f}, which frames the risk-adjusted tradeoff.",
                ],
                metrics={
                    "largest_sector_weight": baseline_bundle.baseline.sector_exposures[0].weight,
                    "herfindahl_index": baseline_bundle.metrics_map["herfindahl_index"],
                    "sharpe_ratio": baseline_bundle.metrics_map["sharpe_ratio"],
                },
            ),
        ]
        return DynamicEDAResult(
            workflow=plan.dynamic_workflow,
            question_type=plan.question_type,
            findings=findings,
            tables=[
                AnalysisTable(
                    name="Correlation Hotspots",
                    columns=["ticker_a", "ticker_b", "correlation"],
                    rows=[
                        {
                            "ticker_a": a,
                            "ticker_b": b,
                            "correlation": round(correlation, 4),
                        }
                        for a, b, correlation in correlation_pairs[:5]
                    ],
                )
            ],
        )

    def _performance(
        self,
        plan: AnalysisPlan,
        baseline_bundle: AnalyticsBundle,
    ) -> DynamicEDAResult:
        contributors = baseline_bundle.baseline.contributors
        top = contributors[:3]
        bottom = sorted(contributors, key=lambda item: item.contribution_pct)[:3]
        findings = [
            EDAFinding(
                headline="Performance is being driven by a small subset of names.",
                evidence=[
                    f"Top contributor is {top[0].ticker} at {top[0].contribution_pct * 100:.2f}% contribution.",
                    f"Trailing return vs benchmark is {baseline_bundle.metrics_map['return_vs_benchmark'] * 100:.2f} percentage points.",
                    f"Worst detractor is {bottom[0].ticker} at {bottom[0].contribution_pct * 100:.2f}% contribution.",
                ],
                metrics={
                    "top_contribution": top[0].contribution_pct,
                    "return_vs_benchmark": baseline_bundle.metrics_map["return_vs_benchmark"],
                    "worst_contribution": bottom[0].contribution_pct,
                },
            ),
            EDAFinding(
                headline="Relative performance combines stock selection with portfolio risk posture.",
                evidence=[
                    f"Portfolio beta is {baseline_bundle.metrics_map['beta_vs_benchmark']:.2f}.",
                    f"Sharpe ratio is {baseline_bundle.metrics_map['sharpe_ratio']:.2f}.",
                    f"Max drawdown reached {baseline_bundle.metrics_map['max_drawdown'] * 100:.2f}%.",
                ],
                metrics={
                    "beta_vs_benchmark": baseline_bundle.metrics_map["beta_vs_benchmark"],
                    "sharpe_ratio": baseline_bundle.metrics_map["sharpe_ratio"],
                    "max_drawdown": baseline_bundle.metrics_map["max_drawdown"],
                },
            ),
        ]
        return DynamicEDAResult(
            workflow=plan.dynamic_workflow,
            question_type=plan.question_type,
            findings=findings,
            tables=[
                AnalysisTable(
                    name="Contribution Decomposition",
                    columns=["ticker", "contribution_pct", "return_pct", "weight"],
                    rows=[item.model_dump() for item in contributors[:8]],
                )
            ],
        )

    async def _rates(
        self,
        plan: AnalysisPlan,
        baseline_bundle: AnalyticsBundle,
    ) -> DynamicEDAResult:
        try:
            treasury = await self.alpha_vantage.get_treasury_yield()
            aligned = pd.concat(
                [
                    baseline_bundle.portfolio_returns.rename("portfolio"),
                    baseline_bundle.benchmark_returns.rename("benchmark"),
                    treasury["value"].diff().rename("yield_change"),
                ],
                axis=1,
            ).dropna()
        except Exception:  # noqa: BLE001
            aligned = pd.DataFrame()
        if aligned.empty:
            findings = [
                EDAFinding(
                    headline="Rates workflow was selected, but the macro series did not return a usable aligned sample.",
                    evidence=[
                        "The app kept the baseline portfolio analytics intact.",
                        "A macro overlay caveat should be applied before drawing rates conclusions.",
                    ],
                    metrics={},
                    severity="warning",
                )
            ]
            return DynamicEDAResult(
                workflow=plan.dynamic_workflow,
                question_type=plan.question_type,
                findings=findings,
                tables=[],
            )
        correlations = aligned.corr()
        yield_days = aligned.sort_values("yield_change", ascending=False).head(20)
        findings = [
            EDAFinding(
                headline="Rates sensitivity is measured from empirical co-movement, not a forecast.",
                evidence=[
                    f"Portfolio daily return correlation to 10Y yield changes is {correlations.loc['portfolio', 'yield_change']:.2f}.",
                    f"Benchmark daily return correlation to 10Y yield changes is {correlations.loc['benchmark', 'yield_change']:.2f}.",
                    f"On the 20 largest yield-up days, the portfolio average return was {yield_days['portfolio'].mean() * 100:.2f}%.",
                ],
                metrics={
                    "portfolio_yield_corr": float(correlations.loc["portfolio", "yield_change"]),
                    "benchmark_yield_corr": float(correlations.loc["benchmark", "yield_change"]),
                    "avg_portfolio_return_top_yield_days": float(yield_days["portfolio"].mean()),
                },
            )
        ]
        return DynamicEDAResult(
            workflow=plan.dynamic_workflow,
            question_type=plan.question_type,
            findings=findings,
            tables=[
                AnalysisTable(
                    name="Rates Regime Sample",
                    columns=["date", "portfolio", "benchmark", "yield_change"],
                    rows=[
                        {
                            "date": index.strftime("%Y-%m-%d"),
                            "portfolio": round(row["portfolio"], 6),
                            "benchmark": round(row["benchmark"], 6),
                            "yield_change": round(row["yield_change"], 6),
                        }
                        for index, row in yield_days.head(10).iterrows()
                    ],
                )
            ],
        )

    async def _war(
        self,
        plan: AnalysisPlan,
        baseline_bundle: AnalyticsBundle,
    ) -> DynamicEDAResult:
        try:
            wti = await self.alpha_vantage.get_wti()
            aligned = pd.concat(
                [
                    baseline_bundle.portfolio_returns.rename("portfolio"),
                    baseline_bundle.benchmark_returns.rename("benchmark"),
                    wti["value"].pct_change().rename("oil_change"),
                ],
                axis=1,
            ).dropna()
        except Exception:  # noqa: BLE001
            aligned = pd.DataFrame()
        if aligned.empty:
            findings = [
                EDAFinding(
                    headline="Geopolitical stress workflow ran, but the oil-shock proxy series was unavailable or too sparse.",
                    evidence=[
                        "The app preserved the baseline portfolio evidence.",
                        "War-scenario interpretation should be treated as incomplete until macro proxy data is available.",
                    ],
                    metrics={},
                    severity="warning",
                )
            ]
            return DynamicEDAResult(
                workflow=plan.dynamic_workflow,
                question_type=plan.question_type,
                findings=findings,
                tables=[],
            )
        shock_days = aligned[(aligned["oil_change"] > aligned["oil_change"].quantile(0.9)) & (aligned["benchmark"] < 0)]
        findings = [
            EDAFinding(
                headline="Geopolitical stress is proxied through oil shock plus equity stress regimes.",
                evidence=[
                    f"The sample contains {len(shock_days)} war-like shock days over the lookback window.",
                    f"Average portfolio return on those days was {shock_days['portfolio'].mean() * 100:.2f}%.",
                    f"Average benchmark return on those days was {shock_days['benchmark'].mean() * 100:.2f}%.",
                ],
                metrics={
                    "shock_day_count": float(len(shock_days)),
                    "portfolio_avg_shock_return": float(shock_days["portfolio"].mean()) if not shock_days.empty else 0.0,
                    "benchmark_avg_shock_return": float(shock_days["benchmark"].mean()) if not shock_days.empty else 0.0,
                },
            )
        ]
        return DynamicEDAResult(
            workflow=plan.dynamic_workflow,
            question_type=plan.question_type,
            findings=findings,
            tables=[
                AnalysisTable(
                    name="Oil Shock Regime",
                    columns=["date", "portfolio", "benchmark", "oil_change"],
                    rows=[
                        {
                            "date": index.strftime("%Y-%m-%d"),
                            "portfolio": round(row["portfolio"], 6),
                            "benchmark": round(row["benchmark"], 6),
                            "oil_change": round(row["oil_change"], 6),
                        }
                        for index, row in shock_days.head(10).iterrows()
                    ],
                )
            ],
        )

    def _what_if(
        self,
        plan: AnalysisPlan,
        baseline_bundle: AnalyticsBundle,
        question: str,
    ) -> DynamicEDAResult:
        findings = [
            EDAFinding(
                headline="What-if analysis is evaluated as a deterministic before/after portfolio comparison.",
                evidence=[
                    f"Baseline top 3 weight is {baseline_bundle.metrics_map['top3_share'] * 100:.2f}%.",
                    f"Baseline Sharpe ratio is {baseline_bundle.metrics_map['sharpe_ratio']:.2f}.",
                    f"Question received: {question}",
                ],
                metrics={
                    "top3_share": baseline_bundle.metrics_map["top3_share"],
                    "sharpe_ratio": baseline_bundle.metrics_map["sharpe_ratio"],
                },
            )
        ]
        return DynamicEDAResult(
            workflow=plan.dynamic_workflow,
            question_type=plan.question_type,
            findings=findings,
            tables=[],
        )
