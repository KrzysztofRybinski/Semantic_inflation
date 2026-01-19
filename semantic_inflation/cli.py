import argparse
import json
from pathlib import Path

from semantic_inflation.config import load_settings
from semantic_inflation.paths import repo_root
from semantic_inflation.text.features import compute_features_from_file


def _cmd_toy(args: argparse.Namespace) -> int:
    settings = load_settings(args.config)
    fixture = repo_root() / "data" / "fixtures" / "sample_filing.html"
    result = compute_features_from_file(
        fixture,
        dictionary_version=settings.text.dictionary_version,
        min_sentence_chars=settings.text.min_sentence_chars,
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


def _cmd_config(args: argparse.Namespace) -> int:
    settings = load_settings(args.config)
    payload = settings.model_dump(mode="json")
    payload["paths"]["resolved"] = settings.resolved_paths()
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

    p_config = sub.add_parser("config", help="Print resolved configuration")
    p_config.set_defaults(func=_cmd_config)

    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    return int(args.func(args))
