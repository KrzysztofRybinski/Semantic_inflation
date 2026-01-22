from __future__ import annotations

import json
from typing import Any

import numpy as np
import pandas as pd
import statsmodels.api as sm
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import roc_auc_score

from semantic_inflation.pipeline.context import PipelineContext
from semantic_inflation.pipeline.io_utils import is_complete, write_json


def _safe_series(df: pd.DataFrame, name: str, default: float = 0.0) -> pd.Series:
    if name in df.columns:
        return pd.to_numeric(df[name], errors="coerce").fillna(default)
    return pd.Series([default] * len(df))


def run_models(context: PipelineContext) -> dict[str, Any]:
    settings = context.settings
    manifest_path = settings.paths.outputs_dir / "manifests" / "models.json"
    output_path = settings.paths.outputs_dir / "results" / "models_summary.json"

    if is_complete(manifest_path, [output_path]):
        return {"skipped": True, "output": str(output_path)}

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

    qc_payload = {
        "rows": len(panel),
        "output": str(output_path),
        "auc": auc,
    }
    write_json(settings.paths.outputs_dir / "qc" / "models_qc.json", qc_payload)

    manifest = {
        "stage": "models",
        "status": "completed",
        "timestamp": context.now_iso(),
        "output": str(output_path),
    }
    write_json(manifest_path, manifest)
    return qc_payload
