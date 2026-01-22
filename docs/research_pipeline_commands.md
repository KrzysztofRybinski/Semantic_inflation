# Research pipeline commands (steps 6–12)

Run the commands **in order**. Each step includes the QC command immediately after the main command.

> **Note on shells:**
> - Commands below are written for **PowerShell** (Windows) to match the pipeline examples (e.g., `Tee-Object`).
> - If you are on **bash/zsh**, see the bash variants included for Steps 8–10.

---

## 6) Verify you have at least one real SEC HTML file to process

**QC command (PowerShell):**

```powershell
@'
from pathlib import Path

p = Path("data/raw/sec/aapl-20240928.htm")
assert p.exists(), f"Missing input file: {p}"
assert p.stat().st_size > 50_000, f"File seems too small: {p.stat().st_size} bytes"
print("QC PASS: found sample filing:", p, "bytes=", p.stat().st_size)
'@ | uv run python -
```

---

## 7) Extract clean text from HTML (library call; works even if CLI changed)

**Command (PowerShell):**

```powershell
@'
from pathlib import Path

inp = Path("data/raw/sec/aapl-20240928.htm")
raw = inp.read_text(encoding="utf-8", errors="ignore")

# Robust call: supports either html_to_text(raw) or html_to_text(raw, **kwargs)
from semantic_inflation.text.clean_html import html_to_text

try:
    txt = html_to_text(raw)
except TypeError:
    # In case Codex added required kwargs
    txt = html_to_text(raw, extractor="bs4")

out = Path("outputs/text/aapl-20240928.txt")
out.parent.mkdir(parents=True, exist_ok=True)
out.write_text(txt, encoding="utf-8")

print("Wrote:", out)
print("Chars:", len(txt))
print("First 200 chars:", txt[:200].replace("\n"," ") )
'@ | uv run python -
```

**QC command (PowerShell):**

```powershell
@'
from pathlib import Path

p = Path("outputs/text/aapl-20240928.txt")
t = p.read_text(encoding="utf-8", errors="ignore")
assert len(t) > 10_000, f"Extracted text too short: {len(t)} chars"

lower = t.lower()
angle = lower.count("<") + lower.count(">")
ratio = angle / max(1, len(lower))

# Heuristic: extracted text should not contain lots of HTML tags
assert ratio < 0.01, f"Too much markup remains: angle_ratio={ratio:.4f}"

# Another heuristic: should not contain full <html> document markers
assert "<html" not in lower, "Still contains <html> tag"

print("QC PASS: extracted text looks sane")
print("Angle bracket ratio:", ratio)
print("Sample tokens present:", [w for w in ["climate","emission","greenhouse","carbon"] if w in lower][:10])
'@ | uv run python -
```

---

## 8) Compute disclosure features (A_share, Q_share, counts, hashes) for the filing

**Command (PowerShell):**

```powershell
uv run python -m semantic_inflation features --input data\raw\sec\aapl-20240928.htm | Tee-Object outputs\features\aapl-20240928.json
```

**Command (bash/zsh equivalent):**

```bash
uv run python -m semantic_inflation features --input data/raw/sec/aapl-20240928.htm | tee outputs/features/aapl-20240928.json
```

**QC command (PowerShell):**

```powershell
@'
import json, re
from pathlib import Path

p = Path("outputs/features/aapl-20240928.json")
d = json.loads(p.read_text(encoding="utf-8"))

required = [
  "A_share","Q_share","dictionary_sha256","dictionary_version",
  "sentences_total","sentences_env","sentences_aspirational","sentences_kpi",
  "input_sha256","env_word_count"
]
missing = [k for k in required if k not in d]
assert not missing, f"Missing keys: {missing}"

assert 0 <= d["A_share"] <= 1, d["A_share"]
assert 0 <= d["Q_share"] <= 1, d["Q_share"]

assert d["sentences_total"] > 300, f"Too few sentences for a 10-K? {d['sentences_total']}"
assert d["sentences_env"] >= 0
assert d["sentences_aspirational"] <= d["sentences_env"]
assert d["sentences_kpi"] <= d["sentences_env"]

for k in ["dictionary_sha256","input_sha256"]:
    assert re.fullmatch(r"[0-9a-f]{64}", d[k]), f"Bad sha256 in {k}: {d[k]}"

print("QC PASS: features output looks valid")
print({k: d[k] for k in ["A_share","Q_share","sentences_env","sentences_kpi","env_word_count"]})
'@ | uv run python -
```

---

## 9) Determinism check (re-run features and compare stable fields)

**Command (PowerShell):**

```powershell
uv run python -m semantic_inflation features --input data\raw\sec\aapl-20240928.htm | Tee-Object outputs\features\aapl-20240928.rerun.json
```

**Command (bash/zsh equivalent):**

```bash
uv run python -m semantic_inflation features --input data/raw/sec/aapl-20240928.htm | tee outputs/features/aapl-20240928.rerun.json
```

**QC command (PowerShell):**

```powershell
@'
import json
from pathlib import Path

a = json.loads(Path("outputs/features/aapl-20240928.json").read_text(encoding="utf-8"))
b = json.loads(Path("outputs/features/aapl-20240928.rerun.json").read_text(encoding="utf-8"))

stable = ["A_share","Q_share","sentences_total","sentences_env","sentences_aspirational","sentences_kpi","dictionary_sha256","input_sha256"]
diff = {k:(a.get(k),b.get(k)) for k in stable if a.get(k)!=b.get(k)}
assert not diff, f"Non-deterministic fields: {diff}"

print("QC PASS: deterministic on stable fields")
'@ | uv run python -
```

---

## 10) Batch compute features for ALL HTML filings in data/raw/sec/

**Command (PowerShell):**

```powershell
Get-ChildItem -Path data\raw\sec -Recurse -Include *.htm,*.html | ForEach-Object {
  $in  = $_.FullName
  $out = Join-Path "outputs\features" ($_.BaseName + ".json")
  Write-Host "FEATURES: $in -> $out"
  uv run python -m semantic_inflation features --input $in | Out-File -Encoding utf8 $out
}
```

**Command (bash/zsh equivalent):**

```bash
find data/raw/sec -type f \( -name "*.htm" -o -name "*.html" \) -print0 | while IFS= read -r -d '' in; do
  base=$(basename "$in")
  out="outputs/features/${base%.*}.json"
  echo "FEATURES: $in -> $out"
  uv run python -m semantic_inflation features --input "$in" | tee "$out" >/dev/null
  # If you prefer not to echo to terminal, replace 'tee' with '>'
  # uv run python -m semantic_inflation features --input "$in" > "$out"
done
```

**QC command (PowerShell):**

```powershell
@'
import json, glob, os, re
from pathlib import Path

paths = sorted(glob.glob(os.path.join("outputs","features","*.json")))
assert paths, "No feature JSON files found in outputs/features/*.json"

required = ["A_share","Q_share","sentences_env","sentences_total","dictionary_sha256","input_sha256"]

bad = []
for p in paths:
    try:
        d = json.loads(Path(p).read_text(encoding="utf-8"))
        for k in required:
            if k not in d:
                raise ValueError(f"missing {k}")
        if not (0 <= d["A_share"] <= 1 and 0 <= d["Q_share"] <= 1):
            raise ValueError("shares out of range")
        if d["sentences_total"] <= 0:
            raise ValueError("sentences_total <= 0")
        if not re.fullmatch(r"[0-9a-f]{64}", d["dictionary_sha256"]):
            raise ValueError("bad dictionary_sha256")
        if not re.fullmatch(r"[0-9a-f]{64}", d["input_sha256"]):
            raise ValueError("bad input_sha256")
    except Exception as e:
        bad.append((p, str(e)))

assert not bad, "Some feature files failed QC:\n" + "\n".join([f"{p}: {e}" for p,e in bad[:20]])
print(f"QC PASS: {len(paths)} feature files validated")
'@ | uv run python -
```

---

## 11) Build a single summary dataset (CSV) + compute a simple SI proxy

**Command (PowerShell):**

```powershell
@'
import csv, glob, json, os
from pathlib import Path

paths = sorted(glob.glob(os.path.join("outputs","features","*.json")))
rows = [json.loads(Path(p).read_text(encoding="utf-8")) for p in paths]
assert rows, "No rows to summarize."

# Add a simple SI proxy for early exploration (not the final industry-year z-score SI):
# SI_simple = A_share - Q_share
for r in rows:
    r["SI_simple"] = (r.get("A_share") or 0.0) - (r.get("Q_share") or 0.0)

out = Path("outputs/features/features_summary.csv")
out.parent.mkdir(parents=True, exist_ok=True)

fieldnames = sorted({k for r in rows for k in r.keys()})
with out.open("w", newline="", encoding="utf-8") as f:
    w = csv.DictWriter(f, fieldnames=fieldnames)
    w.writeheader()
    w.writerows(rows)

print("Wrote:", out, "rows=", len(rows))
'@ | uv run python -
```

**QC command (PowerShell):**

```powershell
@'
import csv
from pathlib import Path
import statistics as st

p = Path("outputs/features/features_summary.csv")
assert p.exists(), "Missing features_summary.csv"

rows = list(csv.DictReader(p.open("r", encoding="utf-8")))
assert rows, "CSV has 0 rows"

def f(x):
    try: return float(x)
    except: return None

A = [f(r.get("A_share")) for r in rows if f(r.get("A_share")) is not None]
Q = [f(r.get("Q_share")) for r in rows if f(r.get("Q_share")) is not None]
KPI = []
for r in rows:
    try: KPI.append(int(float(r.get("sentences_kpi","0"))))
    except: KPI.append(0)

print("Rows:", len(rows))
print("A_share median:", st.median(A) if A else None)
print("Q_share median:", st.median(Q) if Q else None)
print("Share with KPI sentences > 0:", sum(k>0 for k in KPI), "/", len(KPI))

if all(k==0 for k in KPI):
    print("WARNING: KPI detector never fired in this sample.")
    print("  This can be OK if your sample firms simply don't disclose numeric KPIs in 10-Ks.")
    print("  But if you expected KPIs, test with a KPI-heavy industry 10-K and/or review table extraction.")
'@ | uv run python -
```

---

## 12) Record provenance (dictionary hash + git commit)

**Command (PowerShell):**

```powershell
@'
import hashlib, json, subprocess
from pathlib import Path

dict_path = Path("semantic_inflation/resources/dictionaries_v1.toml")
sha = hashlib.sha256(dict_path.read_bytes()).hexdigest()

try:
    commit = subprocess.check_output(["git","rev-parse","HEAD"], text=True).strip()
except Exception:
    commit = None

prov = {
  "git_commit": commit,
  "dictionary_path": str(dict_path),
  "dictionary_sha256": sha,
}

out = Path("outputs/qc/provenance.json")
out.parent.mkdir(parents=True, exist_ok=True)
out.write_text(json.dumps(prov, indent=2), encoding="utf-8")

print("Wrote:", out)
print(prov)
'@ | uv run python -
```
