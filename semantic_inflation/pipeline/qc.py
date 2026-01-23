from __future__ import annotations

from typing import Any

import pandas as pd


def qc_frame(df: pd.DataFrame, *, stage: str) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "stage": stage,
        "rows": int(len(df)),
        "columns": list(df.columns),
    }
    if not df.empty:
        payload["year_min"] = int(pd.to_numeric(df.get("year"), errors="coerce").min()) if "year" in df.columns else None
        payload["year_max"] = int(pd.to_numeric(df.get("year"), errors="coerce").max()) if "year" in df.columns else None
        payload["missingness"] = df.isna().mean().to_dict()
    return payload
