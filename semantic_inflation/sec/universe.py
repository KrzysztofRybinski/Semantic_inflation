from __future__ import annotations

import json
from pathlib import Path
import re
from typing import Any

import pandas as pd
from rapidfuzz import fuzz, process

from semantic_inflation.net.download import download_file


def load_corp_suffixes(path: Path) -> set[str]:
    if not path.exists():
        return set()
    return {
        line.strip().upper()
        for line in path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    }


def normalize_company_name(name: str, suffixes: set[str]) -> str:
    cleaned = re.sub(r"[^\w\s]", " ", str(name).upper())
    tokens = [tok for tok in cleaned.split() if tok and tok not in suffixes]
    return " ".join(tokens)


def download_company_tickers(
    url: str,
    destination: Path,
    headers: dict[str, str],
    max_rps: float,
    manifest_path: Path,
) -> pd.DataFrame:
    download_file(
        url,
        destination,
        headers=headers,
        max_rps=max_rps,
        manifest_path=manifest_path,
    )
    payload = json.loads(destination.read_text(encoding="utf-8"))
    rows = list(payload.values()) if isinstance(payload, dict) else payload
    return pd.DataFrame(rows)


def build_parent_to_cik_crosswalk(
    parent_df: pd.DataFrame,
    sec_df: pd.DataFrame,
    *,
    suffixes: set[str],
    fuzzy_high: int,
    fuzzy_medium: int,
    overrides_path: Path | None = None,
) -> pd.DataFrame:
    parent_df = parent_df.copy()
    parent_df["parent_company_name_norm"] = parent_df["parent_company_name_raw"].astype(str).map(
        lambda value: normalize_company_name(value, suffixes)
    )

    sec_df = sec_df.copy()
    sec_name_col = "sec_name_raw"
    if sec_name_col not in sec_df.columns:
        sec_name_col = "title" if "title" in sec_df.columns else "name"
    sec_df["sec_name_norm"] = sec_df[sec_name_col].astype(str).map(
        lambda value: normalize_company_name(value, suffixes)
    )
    sec_choices = sec_df["sec_name_norm"].tolist()

    matches: list[dict[str, Any]] = []
    for raw, norm in (
        parent_df[["parent_company_name_raw", "parent_company_name_norm"]]
        .drop_duplicates()
        .itertuples(index=False)
    ):
        match = process.extractOne(norm, sec_choices, scorer=fuzz.token_sort_ratio)
        if match:
            match_name, score, idx = match
            sec_row = sec_df.iloc[idx]
            matches.append(
                {
                    "parent_company_name_raw": raw,
                    "parent_company_name_norm": norm,
                    "matched_cik": str(sec_row.get("cik_str") or sec_row.get("cik") or "").zfill(10),
                    "matched_sec_name": sec_row.get(sec_name_col),
                    "match_score": int(score),
                    "match_method": "fuzzy",
                    "match_tier": "high"
                    if score >= fuzzy_high
                    else "medium"
                    if score >= fuzzy_medium
                    else "low",
                    "manual_override": False,
                }
            )
        else:
            matches.append(
                {
                    "parent_company_name_raw": raw,
                    "parent_company_name_norm": norm,
                    "matched_cik": "",
                    "matched_sec_name": "",
                    "match_score": 0,
                    "match_method": "unmatched",
                    "match_tier": "low",
                    "manual_override": False,
                }
            )

    if overrides_path and overrides_path.exists():
        overrides = pd.read_csv(overrides_path)
        rename_map: dict[str, str] = {}
        if "parent_company_name_raw" not in overrides.columns:
            for col in overrides.columns:
                if "parent" in col.lower():
                    rename_map[col] = "parent_company_name_raw"
                    break
        if "matched_cik" not in overrides.columns:
            for col in overrides.columns:
                if "cik" in col.lower():
                    rename_map[col] = "matched_cik"
                    break
        if rename_map:
            overrides = overrides.rename(columns=rename_map)
        if "parent_company_name_raw" in overrides.columns and "matched_cik" in overrides.columns:
            override_map = overrides.set_index("parent_company_name_raw")["matched_cik"].to_dict()
            for row in matches:
                override = override_map.get(row["parent_company_name_raw"])
                if override:
                    row["matched_cik"] = str(override).zfill(10)
                    row["match_method"] = "manual"
                    row["manual_override"] = True
                    row["match_tier"] = "high"

    return pd.DataFrame(matches)


def build_cik_universe(crosswalk_df: pd.DataFrame, tiers: set[str]) -> pd.DataFrame:
    subset = crosswalk_df[crosswalk_df["match_tier"].isin(tiers)].copy()
    subset = subset[subset["matched_cik"].astype(str).str.len() > 0]
    return subset[["matched_cik", "match_tier", "parent_company_name_norm"]].rename(
        columns={"matched_cik": "cik"}
    )
