from __future__ import annotations

from pathlib import Path
from typing import Any
import zipfile

import pandas as pd


def _find_column(columns: list[str], keywords: list[str]) -> str | None:
    lower_cols = {col.lower(): col for col in columns}
    for keyword in keywords:
        for col_lower, original in lower_cols.items():
            if keyword in col_lower:
                return original
    return None


def parse_frs_program_links(zip_path: Path) -> pd.DataFrame:
    with zipfile.ZipFile(zip_path) as archive:
        candidates = [
            name
            for name in archive.namelist()
            if name.lower().endswith(".csv") and "program" in name.lower()
        ]
        if not candidates:
            raise FileNotFoundError("FRS program links CSV not found in FRS downloads zip.")
        chosen = candidates[0]
        with archive.open(chosen) as handle:
            df = pd.read_csv(handle, low_memory=False)

    frs_col = _find_column(df.columns.tolist(), ["registry_id", "frs_id"])
    program_id_col = _find_column(df.columns.tolist(), ["program_sys_id", "program system id"])
    acronym_col = _find_column(df.columns.tolist(), ["program_acronym", "program acronym"])
    if not frs_col or not program_id_col or not acronym_col:
        raise ValueError("FRS program links missing required columns.")

    links = df.rename(
        columns={
            frs_col: "frs_id",
            program_id_col: "program_sys_id",
            acronym_col: "program_acronym",
        }
    )[
        ["frs_id", "program_sys_id", "program_acronym"]
    ].copy()
    links["frs_id"] = links["frs_id"].astype(str)
    links["program_sys_id"] = links["program_sys_id"].astype(str)
    links["program_acronym"] = links["program_acronym"].astype(str)
    return links


def detect_ghgrp_program_acronym(
    program_links: pd.DataFrame, ghgrp_ids: pd.Series
) -> tuple[str | None, dict[str, Any]]:
    ghgrp_set = set(ghgrp_ids.dropna().astype(str))
    if not ghgrp_set:
        return None, {"reason": "empty_ghgrp_ids"}

    scores: dict[str, float] = {}
    for acronym, group in program_links.groupby("program_acronym"):
        program_ids = set(group["program_sys_id"].astype(str))
        if not program_ids:
            continue
        scores[acronym] = len(ghgrp_set.intersection(program_ids)) / len(ghgrp_set)

    if scores:
        best = max(scores.items(), key=lambda item: item[1])
        return best[0], {"match_rates": scores, "selected": best[0]}

    fallback = None
    for acronym in program_links["program_acronym"].unique():
        if "GHG" in acronym.upper():
            fallback = acronym
            break
    return fallback, {"match_rates": scores, "selected": fallback, "fallback": True}


def build_ghgrp_to_frs(program_links: pd.DataFrame, ghgrp_ids: pd.Series) -> pd.DataFrame:
    acronym, _ = detect_ghgrp_program_acronym(program_links, ghgrp_ids)
    if not acronym:
        raise ValueError("Unable to detect GHGRP program acronym from FRS program links.")
    subset = program_links[program_links["program_acronym"] == acronym].copy()
    subset = subset.rename(columns={"program_sys_id": "ghgrp_facility_id"})
    subset = subset[["ghgrp_facility_id", "frs_id"]].dropna()
    subset = subset.drop_duplicates(subset=["ghgrp_facility_id"])
    return subset
