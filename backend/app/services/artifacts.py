from __future__ import annotations

import json
import os
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

MPL_CACHE_DIR = Path(__file__).resolve().parents[2] / ".matplotlib"
MPL_CACHE_DIR.mkdir(parents=True, exist_ok=True)
os.environ.setdefault("MPLCONFIGDIR", str(MPL_CACHE_DIR))

import matplotlib
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

from app.database import Database
from app.models.schemas import ArtifactRecord, BaselineAnalytics, DynamicEDAResult, FinalMemo, ScenarioAnalytics

matplotlib.use("Agg")


class ArtifactService:
    def __init__(self, database: Database, artifacts_dir: Path) -> None:
        self.database = database
        self.artifacts_dir = artifacts_dir

    def create_session_dir(self, session_id: str) -> Path:
        session_dir = self.artifacts_dir / session_id
        session_dir.mkdir(parents=True, exist_ok=True)
        return session_dir

    def save_json_artifact(
        self,
        *,
        session_id: str,
        kind: str,
        title: str,
        payload: dict,
    ) -> ArtifactRecord:
        session_dir = self.create_session_dir(session_id)
        filename = f"{kind}.json"
        target = session_dir / filename
        target.write_text(json.dumps(payload, indent=2), encoding="utf-8")
        return self._register_artifact(session_id=session_id, kind=kind, title=title, path=target)

    def save_markdown_memo(
        self,
        *,
        session_id: str,
        memo: FinalMemo,
    ) -> ArtifactRecord:
        session_dir = self.create_session_dir(session_id)
        target = session_dir / "final_memo.md"
        lines = [
            f"# {memo.title}",
            "",
            f"## Thesis\n{memo.thesis}",
            "",
            "## Executive Summary",
            *[f"- {item}" for item in memo.executive_summary],
            "",
            "## Evidence",
            *[f"- {item}" for item in memo.evidence],
            "",
            "## Risks And Caveats",
            *[f"- {item}" for item in memo.risks_and_caveats],
            "",
            "## Next Steps",
            *[f"- {item}" for item in memo.next_steps],
        ]
        target.write_text("\n".join(lines), encoding="utf-8")
        return self._register_artifact(
            session_id=session_id,
            kind="final_memo",
            title="Final memo",
            path=target,
        )

    def generate_baseline_charts(
        self,
        *,
        session_id: str,
        baseline: BaselineAnalytics,
    ) -> list[ArtifactRecord]:
        session_dir = self.create_session_dir(session_id)
        records = [
            self._plot_cumulative_performance(session_id, session_dir, baseline),
            self._plot_sector_exposure(session_id, session_dir, baseline),
            self._plot_correlation_heatmap(session_id, session_dir, baseline),
        ]
        return records

    def generate_scenario_chart(
        self,
        *,
        session_id: str,
        scenario: ScenarioAnalytics,
    ) -> ArtifactRecord:
        session_dir = self.create_session_dir(session_id)
        before_map = {item.key: item.value for item in scenario.before_metrics}
        after_map = {item.key: item.value for item in scenario.after_metrics}
        keys = ["annualized_volatility", "beta_vs_benchmark", "sharpe_ratio", "top3_share"]
        labels = ["Volatility", "Beta", "Sharpe", "Top 3 Weight"]
        before_values = [before_map.get(key) or 0.0 for key in keys]
        after_values = [after_map.get(key) or 0.0 for key in keys]

        fig, ax = plt.subplots(figsize=(10, 5))
        x = np.arange(len(keys))
        width = 0.35
        ax.bar(x - width / 2, before_values, width=width, label="Before", color="#243b53")
        ax.bar(x + width / 2, after_values, width=width, label="After", color="#d9822b")
        ax.set_xticks(x)
        ax.set_xticklabels(labels)
        ax.set_title(f"Scenario Comparison: {scenario.label}")
        ax.legend()
        fig.tight_layout()
        target = session_dir / "scenario_comparison.png"
        fig.savefig(target, dpi=160)
        plt.close(fig)
        return self._register_artifact(
            session_id=session_id,
            kind="scenario_chart",
            title="Scenario comparison chart",
            path=target,
        )

    def save_session_result(
        self,
        *,
        session_id: str,
        question: str,
        portfolio_json: dict,
        plan_json: dict,
        result_json: dict,
    ) -> None:
        with self.database.connect() as connection:
            connection.execute(
                """
                INSERT INTO analysis_sessions(session_id, created_at, question, portfolio_json, plan_json, result_json)
                VALUES(?, ?, ?, ?, ?, ?)
                ON CONFLICT(session_id) DO UPDATE SET
                  question = excluded.question,
                  portfolio_json = excluded.portfolio_json,
                  plan_json = excluded.plan_json,
                  result_json = excluded.result_json
                """,
                (
                    session_id,
                    datetime.now(UTC).isoformat(),
                    question,
                    json.dumps(portfolio_json),
                    json.dumps(plan_json),
                    json.dumps(result_json),
                ),
            )

    def _plot_cumulative_performance(
        self,
        session_id: str,
        session_dir: Path,
        baseline: BaselineAnalytics,
    ) -> ArtifactRecord:
        frame = pd.DataFrame([item.model_dump() for item in baseline.performance_series])
        fig, ax = plt.subplots(figsize=(10, 5))
        ax.plot(frame["date"], frame["portfolio_index"], label="Portfolio", color="#243b53", linewidth=2.0)
        ax.plot(frame["date"], frame["benchmark_index"], label=baseline.benchmark_symbol, color="#d9822b", linewidth=2.0)
        ax.set_title("Portfolio vs Benchmark Cumulative Performance")
        ax.tick_params(axis="x", rotation=45)
        ax.legend()
        fig.tight_layout()
        target = session_dir / "cumulative_performance.png"
        fig.savefig(target, dpi=160)
        plt.close(fig)
        return self._register_artifact(
            session_id=session_id,
            kind="cumulative_performance",
            title="Cumulative performance vs benchmark",
            path=target,
        )

    def _plot_sector_exposure(
        self,
        session_id: str,
        session_dir: Path,
        baseline: BaselineAnalytics,
    ) -> ArtifactRecord:
        frame = pd.DataFrame([item.model_dump() for item in baseline.sector_exposures])
        fig, ax = plt.subplots(figsize=(10, 5))
        ax.bar(frame["sector"], frame["weight"], color="#0f8b8d")
        ax.set_title("Sector Exposure")
        ax.tick_params(axis="x", rotation=35)
        ax.set_ylabel("Weight")
        fig.tight_layout()
        target = session_dir / "sector_exposure.png"
        fig.savefig(target, dpi=160)
        plt.close(fig)
        return self._register_artifact(
            session_id=session_id,
            kind="sector_exposure",
            title="Sector exposure chart",
            path=target,
        )

    def _plot_correlation_heatmap(
        self,
        session_id: str,
        session_dir: Path,
        baseline: BaselineAnalytics,
    ) -> ArtifactRecord:
        matrix = pd.DataFrame(baseline.correlation_matrix)
        fig, ax = plt.subplots(figsize=(7, 6))
        heatmap = ax.imshow(matrix.values, cmap="coolwarm", vmin=-1, vmax=1)
        ax.set_xticks(range(len(matrix.columns)))
        ax.set_xticklabels(matrix.columns, rotation=45, ha="right")
        ax.set_yticks(range(len(matrix.index)))
        ax.set_yticklabels(matrix.index)
        ax.set_title("Holding Correlation Heatmap")
        fig.colorbar(heatmap, ax=ax, fraction=0.046, pad=0.04)
        fig.tight_layout()
        target = session_dir / "correlation_heatmap.png"
        fig.savefig(target, dpi=160)
        plt.close(fig)
        return self._register_artifact(
            session_id=session_id,
            kind="correlation_heatmap",
            title="Correlation heatmap",
            path=target,
        )

    def _register_artifact(
        self,
        *,
        session_id: str,
        kind: str,
        title: str,
        path: Path,
    ) -> ArtifactRecord:
        artifact_id = uuid4().hex
        record = ArtifactRecord(
            artifact_id=artifact_id,
            kind=kind,
            title=title,
            path=str(path),
            url=f"/artifacts/{session_id}/{path.name}",
        )
        with self.database.connect() as connection:
            connection.execute(
                """
                INSERT INTO artifacts(artifact_id, session_id, kind, path, created_at, metadata_json)
                VALUES(?, ?, ?, ?, ?, ?)
                """,
                (
                    artifact_id,
                    session_id,
                    kind,
                    str(path),
                    datetime.now(UTC).isoformat(),
                    json.dumps({"title": title}),
                ),
            )
        return record
