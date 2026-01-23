import argparse
import json
from pathlib import Path

from semantic_inflation.config import load_settings
from semantic_inflation.paths import repo_root
from semantic_inflation.pipeline import PipelineContext, run_doctor, run_all
from semantic_inflation.pipeline.echo import download_echo
from semantic_inflation.pipeline.features import compute_sec_features
from semantic_inflation.pipeline.ghgrp import download_ghgrp
from semantic_inflation.pipeline.linkage import build_linkage
from semantic_inflation.pipeline.models import run_classifier, run_regressions
from semantic_inflation.pipeline.panel import build_panel
from semantic_inflation.pipeline.sec import download_sec_filings
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
    payload = run_doctor(context, force=args.force)
    print(json.dumps(payload.to_dict(), indent=2, sort_keys=True))
    return 0


def _cmd_run_all(args: argparse.Namespace) -> int:
    settings = load_settings(args.config)
    context = PipelineContext(settings)
    payload = run_all(context, force=args.force)
    print(json.dumps(payload, indent=2, sort_keys=True))
    return 0


def _cmd_sec_download(args: argparse.Namespace) -> int:
    settings = load_settings(args.config)
    context = PipelineContext(settings)
    payload = download_sec_filings(context, force=args.force)
    print(json.dumps(payload.to_dict(), indent=2, sort_keys=True))
    return 0


def _cmd_sec_features(args: argparse.Namespace) -> int:
    settings = load_settings(args.config)
    context = PipelineContext(settings)
    payload = compute_sec_features(context, force=args.force)
    print(json.dumps(payload.to_dict(), indent=2, sort_keys=True))
    return 0


def _cmd_ghgrp_download(args: argparse.Namespace) -> int:
    settings = load_settings(args.config)
    context = PipelineContext(settings)
    payload = download_ghgrp(context, force=args.force)
    print(json.dumps(payload.to_dict(), indent=2, sort_keys=True))
    return 0


def _cmd_echo_download(args: argparse.Namespace) -> int:
    settings = load_settings(args.config)
    context = PipelineContext(settings)
    payload = download_echo(context, force=args.force)
    print(json.dumps(payload.to_dict(), indent=2, sort_keys=True))
    return 0


def _cmd_link_build(args: argparse.Namespace) -> int:
    settings = load_settings(args.config)
    context = PipelineContext(settings)
    payload = build_linkage(context, force=args.force)
    print(json.dumps(payload.to_dict(), indent=2, sort_keys=True))
    return 0


def _cmd_panel_build(args: argparse.Namespace) -> int:
    settings = load_settings(args.config)
    context = PipelineContext(settings)
    payload = build_panel(context, force=args.force)
    print(json.dumps(payload.to_dict(), indent=2, sort_keys=True))
    return 0


def _cmd_analyze_regressions(args: argparse.Namespace) -> int:
    settings = load_settings(args.config)
    context = PipelineContext(settings)
    payload = run_regressions(context, force=args.force)
    print(json.dumps(payload.to_dict(), indent=2, sort_keys=True))
    return 0


def _cmd_analyze_classifier(args: argparse.Namespace) -> int:
    settings = load_settings(args.config)
    context = PipelineContext(settings)
    payload = run_classifier(context, force=args.force)
    print(json.dumps(payload.to_dict(), indent=2, sort_keys=True))
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="semantic_inflation")
    config_parent = argparse.ArgumentParser(add_help=False)
    config_parent.add_argument(
        "--config",
        default=argparse.SUPPRESS,
        help="Path to TOML config file",
    )
    config_parent.add_argument(
        "--force",
        action="store_true",
        help="Rebuild stage outputs even if manifests exist.",
    )
    parser.add_argument(
        "--config",
        default=str(repo_root() / "configs" / "default.toml"),
        help="Path to TOML config file",
    )

    sub = parser.add_subparsers(dest="command", required=True)

    p_toy = sub.add_parser(
        "toy",
        help="Run feature extraction on a small fixture",
        parents=[config_parent],
    )
    p_toy.set_defaults(func=_cmd_toy)

    p_feat = sub.add_parser(
        "features",
        help="Extract features from local filing files",
        parents=[config_parent],
    )
    p_feat.add_argument("--input", nargs="+", required=True, help="Input HTML/text files")
    p_feat.add_argument("--output", help="Optional JSONL output path")
    p_feat.set_defaults(func=_cmd_features)

    p_extract = sub.add_parser(
        "extract-text",
        help="Extract clean text from HTML filings for debugging",
        parents=[config_parent],
    )
    p_extract.add_argument("--input", required=True, help="Input HTML/text file")
    p_extract.add_argument("--output", help="Optional output path for extracted text")
    p_extract.set_defaults(func=_cmd_extract_text)

    p_config = sub.add_parser(
        "config",
        help="Print resolved configuration",
        parents=[config_parent],
    )
    p_config.set_defaults(func=_cmd_config)

    p_doctor = sub.add_parser(
        "doctor",
        help="Run preflight checks and safe fixes",
        parents=[config_parent],
    )
    p_doctor.set_defaults(func=_cmd_doctor)

    p_run_all = sub.add_parser(
        "run-all",
        help="Run the full semantic inflation research pipeline",
        parents=[config_parent],
    )
    p_run_all.set_defaults(func=_cmd_run_all)

    p_sec = sub.add_parser("sec", help="SEC ingestion commands", parents=[config_parent])
    sec_sub = p_sec.add_subparsers(dest="sec_command", required=True)
    p_sec_download = sec_sub.add_parser(
        "download", help="Download SEC filings", parents=[config_parent]
    )
    p_sec_download.set_defaults(func=_cmd_sec_download)
    p_sec_features = sec_sub.add_parser(
        "features", help="Compute SEC features", parents=[config_parent]
    )
    p_sec_features.set_defaults(func=_cmd_sec_features)

    p_epa = sub.add_parser("epa", help="EPA ingestion commands", parents=[config_parent])
    epa_sub = p_epa.add_subparsers(dest="epa_command", required=True)
    p_epa_ghgrp = epa_sub.add_parser("ghgrp", help="GHGRP ingestion", parents=[config_parent])
    ghgrp_sub = p_epa_ghgrp.add_subparsers(dest="ghgrp_command", required=True)
    p_ghgrp_download = ghgrp_sub.add_parser(
        "download", help="Download GHGRP data", parents=[config_parent]
    )
    p_ghgrp_download.set_defaults(func=_cmd_ghgrp_download)

    p_epa_echo = epa_sub.add_parser("echo", help="ECHO ingestion", parents=[config_parent])
    echo_sub = p_epa_echo.add_subparsers(dest="echo_command", required=True)
    p_echo_download = echo_sub.add_parser(
        "download", help="Download ECHO data", parents=[config_parent]
    )
    p_echo_download.set_defaults(func=_cmd_echo_download)

    p_link = sub.add_parser("link", help="Linkage commands", parents=[config_parent])
    link_sub = p_link.add_subparsers(dest="link_command", required=True)
    p_link_build = link_sub.add_parser("build", help="Build linkage tables", parents=[config_parent])
    p_link_build.set_defaults(func=_cmd_link_build)

    p_panel = sub.add_parser("panel", help="Panel assembly commands", parents=[config_parent])
    panel_sub = p_panel.add_subparsers(dest="panel_command", required=True)
    p_panel_build = panel_sub.add_parser("build", help="Build analysis panel", parents=[config_parent])
    p_panel_build.set_defaults(func=_cmd_panel_build)

    p_analyze = sub.add_parser("analyze", help="Analysis commands", parents=[config_parent])
    analyze_sub = p_analyze.add_subparsers(dest="analyze_command", required=True)
    p_reg = analyze_sub.add_parser("regressions", help="Run regressions", parents=[config_parent])
    p_reg.set_defaults(func=_cmd_analyze_regressions)
    p_clf = analyze_sub.add_parser("classifier", help="Run classifier", parents=[config_parent])
    p_clf.set_defaults(func=_cmd_analyze_classifier)

    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    return int(args.func(args))
