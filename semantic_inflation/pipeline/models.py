from __future__ import annotations

import json
from typing import Any

import numpy as np
import pandas as pd
import statsmodels.api as sm
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import roc_auc_score

from semantic_inflation.pipeline.context import PipelineContext
from semantic_inflation.pipeline.io import write_json
from semantic_inflation.pipeline.state import (
    StageResult,
    compute_inputs_hash,
    should_skip_stage,
    stage_manifest_path,
    write_stage_manifest,
)


def _safe_series(df: pd.DataFrame, name: str, default: float = 0.0) -> pd.Series:
    if name in df.columns:
        return pd.to_numeric(df[name], errors="coerce").fillna(default)
    return pd.Series([default] * len(df))


def run_models(context: PipelineContext, force: bool = False) -> StageResult:
    settings = context.settings
    output_path = settings.paths.outputs_dir / "results" / "models_summary.json"
    inputs_hash = compute_inputs_hash({"stage": "models", "config": settings.model_dump(mode="json")})
    manifest_path = stage_manifest_path(settings.paths.outputs_dir, "models")
    if should_skip_stage(manifest_path, [output_path], inputs_hash, force):
        return StageResult(
            name="models",
            status="skipped",
            outputs=[str(output_path)],
            inputs_hash=inputs_hash,
            stats={"skipped": True},
        )

    panel = pd.read_parquet(settings.paths.processed_dir / "panel.parquet")

    si = _safe_series(panel, "si_simple")
    emissions = _safe_series(panel, "emissions_mtco2e")
    enforcement = _safe_series(panel, "enforcement_action_count")

    X = sm.add_constant(pd.DataFrame({"emissions_mtco2e": emissions}))
    model = sm.OLS(si, X, missing="drop")
    results = model.fit()

    placebo = sm.OLS(si.sample(frac=1.0, random_state=42).reset_index(drop=True), X).fit()

    clf_target = (enforcement > 0).astype(int)
    clf_features = pd.DataFrame({"si_simple": si, "emissions_mtco2e": emissions})
    clf = LogisticRegression(max_iter=1000)
    clf.fit(clf_features, clf_target)
    preds = clf.predict_proba(clf_features)[:, 1]
    auc = roc_auc_score(clf_target, preds) if len(set(clf_target)) > 1 else None

    summary = {
        "ols": {
            "params": results.params.to_dict(),
            "pvalues": results.pvalues.to_dict(),
            "r2": results.rsquared,
        },
        "placebo": {
            "params": placebo.params.to_dict(),
            "pvalues": placebo.pvalues.to_dict(),
            "r2": placebo.rsquared,
        },
        "classifier": {
            "coef": clf.coef_.tolist(),
            "intercept": clf.intercept_.tolist(),
            "auc": auc,
        },
    }

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(summary, indent=2, sort_keys=True), encoding="utf-8")

    qc_payload: dict[str, Any] = {
        "rows": len(panel),
        "output": str(output_path),
        "auc": auc,
    }
    qc_path = settings.paths.outputs_dir / "qc" / "models.json"
    write_json(qc_path, qc_payload)

    result = StageResult(
        name="models",
        status="completed",
        outputs=[str(output_path)],
        qc_path=str(qc_path),
        stats=qc_payload,
        inputs_hash=inputs_hash,
    )
    write_stage_manifest(manifest_path, result)
    return result


def run_regressions(context: PipelineContext, force: bool = False) -> StageResult:
    return run_models(context, force=force)


def run_classifier(context: PipelineContext, force: bool = False) -> StageResult:
    return run_models(context, force=force)
