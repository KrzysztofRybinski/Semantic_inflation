import argparse
import json
from pathlib import Path

from semantic_inflation.config import load_settings
from semantic_inflation.paths import repo_root
from semantic_inflation.pipeline import PipelineContext, run_doctor, run_all
from semantic_inflation.text.clean_html import html_to_text
from semantic_inflation.text.features import compute_features_from_file


def _cmd_toy(args: argparse.Namespace) -> int:
    settings = load_settings(args.config)
    fixture = repo_root() / "data" / "fixtures" / "sample_filing.html"
    result = compute_features_from_file(
        fixture,
        dictionary_version=settings.text.dictionary_version,
        min_sentence_chars=settings.text.min_sentence_chars,
        html_extractor=settings.text.html.extractor,
        drop_hidden=settings.text.html.drop_hidden,
        drop_ix_hidden=settings.text.html.drop_ix_hidden,
        unwrap_ix_tags=settings.text.html.unwrap_ix_tags,
        keep_tables=settings.text.html.keep_tables,
        table_cell_sep=settings.text.html.table_cell_sep,
        table_row_sep=settings.text.html.table_row_sep,
    )
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


def _cmd_features(args: argparse.Namespace) -> int:
    settings = load_settings(args.config)
    inputs: list[Path] = [Path(p) for p in args.input]
    results = [
        compute_features_from_file(
            path,
            dictionary_version=settings.text.dictionary_version,
            min_sentence_chars=settings.text.min_sentence_chars,
            html_extractor=settings.text.html.extractor,
            drop_hidden=settings.text.html.drop_hidden,
            drop_ix_hidden=settings.text.html.drop_ix_hidden,
            unwrap_ix_tags=settings.text.html.unwrap_ix_tags,
            keep_tables=settings.text.html.keep_tables,
            table_cell_sep=settings.text.html.table_cell_sep,
            table_row_sep=settings.text.html.table_row_sep,
        )
        for path in inputs
    ]

    if args.output:
        out_path = Path(args.output)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(
            "\n".join(json.dumps(r, sort_keys=True) for r in results) + "\n",
            encoding="utf-8",
        )
    else:
        for r in results:
            print(json.dumps(r, sort_keys=True))
    return 0


def _cmd_extract_text(args: argparse.Namespace) -> int:
    settings = load_settings(args.config)
    input_path = Path(args.input)
    raw = input_path.read_text(encoding="utf-8", errors="replace")
    if input_path.suffix.lower() in {".html", ".htm"}:
        text = html_to_text(
            raw,
            extractor=settings.text.html.extractor,
            drop_hidden=settings.text.html.drop_hidden,
            drop_ix_hidden=settings.text.html.drop_ix_hidden,
            unwrap_ix_tags=settings.text.html.unwrap_ix_tags,
            keep_tables=settings.text.html.keep_tables,
            table_cell_sep=settings.text.html.table_cell_sep,
            table_row_sep=settings.text.html.table_row_sep,
        )
    else:
        text = raw

    if args.output:
        out_path = Path(args.output)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(text, encoding="utf-8")
    else:
        print(text)
    return 0


def _cmd_config(args: argparse.Namespace) -> int:
    settings = load_settings(args.config)
    payload = settings.model_dump(mode="json")
    payload["paths"]["resolved"] = settings.resolved_paths()
    print(json.dumps(payload, indent=2, sort_keys=True))
    return 0


def _cmd_doctor(args: argparse.Namespace) -> int:
    settings = load_settings(args.config)
    context = PipelineContext(settings)
    payload = run_doctor(context)
    print(json.dumps(payload, indent=2, sort_keys=True))
    return 0


def _cmd_run_all(args: argparse.Namespace) -> int:
    settings = load_settings(args.config)
    context = PipelineContext(settings)
    payload = run_all(context)
    print(json.dumps(payload, indent=2, sort_keys=True))
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="semantic_inflation")
    parser.add_argument(
        "--config",
        default=str(repo_root() / "configs" / "default.toml"),
        help="Path to TOML config file",
    )

    sub = parser.add_subparsers(dest="command", required=True)

    p_toy = sub.add_parser("toy", help="Run feature extraction on a small fixture")
    p_toy.set_defaults(func=_cmd_toy)

    p_feat = sub.add_parser("features", help="Extract features from local filing files")
    p_feat.add_argument("--input", nargs="+", required=True, help="Input HTML/text files")
    p_feat.add_argument("--output", help="Optional JSONL output path")
    p_feat.set_defaults(func=_cmd_features)

    p_extract = sub.add_parser(
        "extract-text", help="Extract clean text from HTML filings for debugging"
    )
    p_extract.add_argument("--input", required=True, help="Input HTML/text file")
    p_extract.add_argument("--output", help="Optional output path for extracted text")
    p_extract.set_defaults(func=_cmd_extract_text)

    p_config = sub.add_parser("config", help="Print resolved configuration")
    p_config.set_defaults(func=_cmd_config)

    p_doctor = sub.add_parser("doctor", help="Run preflight checks and safe fixes")
    p_doctor.set_defaults(func=_cmd_doctor)

    p_run_all = sub.add_parser(
        "run-all",
        help="Run the full semantic inflation research pipeline",
    )
    p_run_all.set_defaults(func=_cmd_run_all)

    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    return int(args.func(args))
