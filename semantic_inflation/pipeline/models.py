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


def _safe_r2(results: sm.regression.linear_model.RegressionResultsWrapper) -> float | None:
    centered_tss = results.centered_tss
    if centered_tss is None or np.isclose(centered_tss, 0.0):
        return None
    return 1.0 - (results.ssr / centered_tss)


def _has_variation(series: pd.Series) -> bool:
    return series.nunique(dropna=True) > 1


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

    warnings: list[str] = []
    can_fit_ols = len(panel) >= 2 and _has_variation(si)
    if can_fit_ols:
        X = sm.add_constant(pd.DataFrame({"emissions_mtco2e": emissions}))
        model = sm.OLS(si, X, missing="drop")
        results = model.fit()
        placebo = sm.OLS(si.sample(frac=1.0, random_state=42).reset_index(drop=True), X).fit()
        ols_summary = {
            "params": results.params.to_dict(),
            "pvalues": results.pvalues.to_dict(),
            "r2": _safe_r2(results),
        }
        placebo_summary = {
            "params": placebo.params.to_dict(),
            "pvalues": placebo.pvalues.to_dict(),
            "r2": _safe_r2(placebo),
        }
    else:
        warnings.append(
            "OLS skipped because the panel has fewer than 2 rows or no variation in the target."
        )
        ols_summary = {
            "params": None,
            "pvalues": None,
            "r2": None,
            "note": "OLS skipped due to insufficient variation in si_simple.",
        }
        placebo_summary = {
            "params": None,
            "pvalues": None,
            "r2": None,
            "note": "Placebo regression skipped because OLS was skipped.",
        }

    clf_target = (enforcement > 0).astype(int)
    clf_features = pd.DataFrame({"si_simple": si, "emissions_mtco2e": emissions})
    class_counts = clf_target.value_counts().to_dict()
    if len(class_counts) > 1 and len(clf_target) >= 2:
        try:
            clf = LogisticRegression(max_iter=1000)
            clf.fit(clf_features, clf_target)
            preds = clf.predict_proba(clf_features)[:, 1]
            auc = roc_auc_score(clf_target, preds)
            classifier_summary = {
                "coef": clf.coef_.tolist(),
                "intercept": clf.intercept_.tolist(),
                "auc": auc,
            }
        except ValueError as exc:
            auc = None
            warnings.append(f"Classifier skipped: {exc}")
            classifier_summary = {
                "coef": None,
                "intercept": None,
                "auc": auc,
                "note": "Classifier skipped due to insufficient class balance.",
            }
    else:
        auc = None
        classifier_summary = {
            "coef": None,
            "intercept": None,
            "auc": auc,
            "note": "Classifier skipped because only one target class is present.",
        }

    summary = {
        "ols": ols_summary,
        "placebo": placebo_summary,
        "classifier": classifier_summary,
    }

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(summary, indent=2, sort_keys=True), encoding="utf-8")

    qc_payload: dict[str, Any] = {
        "rows": len(panel),
        "output": str(output_path),
        "auc": auc,
        "class_counts": class_counts,
    }
    qc_path = settings.paths.outputs_dir / "qc" / "models.json"
    write_json(qc_path, qc_payload)

    result = StageResult(
        name="models",
        status="completed",
        outputs=[str(output_path)],
        qc_path=str(qc_path),
        warnings=warnings,
        stats=qc_payload,
        inputs_hash=inputs_hash,
    )
    write_stage_manifest(manifest_path, result)
    return result


def run_regressions(context: PipelineContext, force: bool = False) -> StageResult:
    return run_models(context, force=force)


def run_classifier(context: PipelineContext, force: bool = False) -> StageResult:
    return run_models(context, force=force)
