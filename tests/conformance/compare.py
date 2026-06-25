#!/usr/bin/env python3
"""Compare cross-SDK conformance harness results.

Each SDK harness emits a JSON document with this shape:

  {"sdk": "rust", "results": [{"scenario": "...", "pass": true,
    "expected": {...}, "observed": {...}}]}

The comparator checks every result against the shared scenario expectations and
then checks the observed values across SDKs. It exits non-zero for missing files,
missing scenarios, failed harness results, expectation mismatches, or cross-SDK
divergences.
"""
import argparse
import json
import sys
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[2]
DEFAULT_SCENARIOS = ROOT / "tests" / "conformance" / "scenarios.json"
DEFAULT_RESULTS = ROOT / "tests" / "conformance" / "results"
DEFAULT_SDK_RESULTS = {
    "rust": DEFAULT_RESULTS / "rust.json",
    "gleam": DEFAULT_RESULTS / "gleam.json",
    "typescript": DEFAULT_RESULTS / "typescript.json",
}


class ComparisonError(Exception):
    """Raised when a conformance input is malformed."""


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Compare liminal SDK conformance result JSON files."
    )
    parser.add_argument(
        "--scenarios",
        type=Path,
        default=DEFAULT_SCENARIOS,
        help=f"shared scenario file (default: {DEFAULT_SCENARIOS})",
    )
    parser.add_argument(
        "--rust",
        type=Path,
        default=DEFAULT_SDK_RESULTS["rust"],
        help=f"Rust result JSON (default: {DEFAULT_SDK_RESULTS['rust']})",
    )
    parser.add_argument(
        "--gleam",
        type=Path,
        default=DEFAULT_SDK_RESULTS["gleam"],
        help=f"Gleam result JSON (default: {DEFAULT_SDK_RESULTS['gleam']})",
    )
    parser.add_argument(
        "--typescript",
        type=Path,
        default=DEFAULT_SDK_RESULTS["typescript"],
        help=(
            "TypeScript result JSON "
            f"(default: {DEFAULT_SDK_RESULTS['typescript']})"
        ),
    )
    return parser.parse_args()


def load_json(path: Path) -> Any:
    try:
        with path.open("r", encoding="utf-8") as handle:
            return json.load(handle)
    except FileNotFoundError as exc:
        raise ComparisonError(f"missing file: {path}") from exc
    except json.JSONDecodeError as exc:
        raise ComparisonError(f"invalid JSON in {path}: {exc}") from exc


def load_expected(path: Path) -> dict[str, Any]:
    document = load_json(path)
    scenarios = document.get("scenarios") if isinstance(document, dict) else None
    if not isinstance(scenarios, list):
        raise ComparisonError(f"{path}: root.scenarios must be an array")

    expected: dict[str, Any] = {}
    for index, scenario in enumerate(scenarios):
        if not isinstance(scenario, dict):
            raise ComparisonError(f"{path}: scenarios/{index} must be an object")
        name = scenario.get("name")
        if not isinstance(name, str):
            raise ComparisonError(f"{path}: scenarios/{index}/name must be a string")
        if name in expected:
            raise ComparisonError(f"{path}: duplicate scenario name {name}")
        if "expected" not in scenario:
            raise ComparisonError(f"{path}: scenario {name} missing expected")
        expected[name] = scenario["expected"]
    return expected


def load_results(sdk_name: str, path: Path) -> dict[str, dict[str, Any]]:
    document = load_json(path)
    if not isinstance(document, dict):
        raise ComparisonError(f"{path}: root must be an object")
    sdk = document.get("sdk")
    if sdk != sdk_name:
        raise ComparisonError(
            f"{path}: sdk must be {sdk_name!r}, got {sdk!r}"
        )
    results = document.get("results")
    if not isinstance(results, list):
        raise ComparisonError(f"{path}: results must be an array")

    indexed: dict[str, dict[str, Any]] = {}
    for index, result in enumerate(results):
        if not isinstance(result, dict):
            raise ComparisonError(f"{path}: results/{index} must be an object")
        scenario = result.get("scenario")
        if not isinstance(scenario, str):
            raise ComparisonError(
                f"{path}: results/{index}/scenario must be a string"
            )
        if scenario in indexed:
            raise ComparisonError(f"{path}: duplicate result for scenario {scenario}")
        for field in ("pass", "expected", "observed"):
            if field not in result:
                raise ComparisonError(
                    f"{path}: result for {scenario} missing {field}"
                )
        if not isinstance(result["pass"], bool):
            raise ComparisonError(f"{path}: result for {scenario} pass must be bool")
        indexed[scenario] = result
    return indexed


def compare(
    expected: dict[str, Any],
    results_by_sdk: dict[str, dict[str, dict[str, Any]]],
) -> list[str]:
    errors: list[str] = []

    for scenario, expected_value in expected.items():
        observed_by_sdk: dict[str, Any] = {}
        for sdk, results in results_by_sdk.items():
            result = results.get(scenario)
            if result is None:
                errors.append(
                    f"SDK={sdk} scenario={scenario} missing result "
                    f"expected={canonical(expected_value)} observed=<missing>"
                )
                continue

            observed = result["observed"]
            observed_by_sdk[sdk] = observed
            if not result["pass"]:
                errors.append(
                    f"SDK={sdk} scenario={scenario} harness reported failure "
                    f"expected={canonical(result['expected'])} "
                    f"observed={canonical(observed)}"
                )
            if result["expected"] != expected_value:
                errors.append(
                    f"SDK={sdk} scenario={scenario} expected value diverged "
                    f"expected={canonical(expected_value)} "
                    f"observed={canonical(result['expected'])}"
                )
            if observed != expected_value:
                errors.append(
                    f"SDK={sdk} scenario={scenario} expected={canonical(expected_value)} "
                    f"observed={canonical(observed)}"
                )

        errors.extend(compare_observed_across_sdks(scenario, expected_value, observed_by_sdk))

    expected_names = set(expected)
    for sdk, results in results_by_sdk.items():
        for scenario in sorted(set(results) - expected_names):
            errors.append(
                f"SDK={sdk} scenario={scenario} unexpected result "
                f"expected=<none> observed={canonical(results[scenario]['observed'])}"
            )

    return errors


def compare_observed_across_sdks(
    scenario: str,
    expected_value: Any,
    observed_by_sdk: dict[str, Any],
) -> list[str]:
    if not observed_by_sdk:
        return []

    baseline_sdk, baseline = next(iter(observed_by_sdk.items()))
    errors: list[str] = []
    for sdk, observed in observed_by_sdk.items():
        if observed != baseline:
            errors.append(
                f"SDK={sdk} scenario={scenario} cross-sdk divergence "
                f"expected={canonical(expected_value)} observed={canonical(observed)} "
                f"baseline_sdk={baseline_sdk} baseline={canonical(baseline)}"
            )
    return errors


def canonical(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"))


def main() -> int:
    args = parse_args()
    try:
        expected = load_expected(args.scenarios)
        results_by_sdk = {
            "rust": load_results("rust", args.rust),
            "gleam": load_results("gleam", args.gleam),
            "typescript": load_results("typescript", args.typescript),
        }
    except ComparisonError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2

    errors = compare(expected, results_by_sdk)
    if errors:
        for error in errors:
            print(error, file=sys.stderr)
        print(f"conformance comparison failed with {len(errors)} divergence(s)", file=sys.stderr)
        return 1

    print(
        "conformance comparison passed: "
        f"{len(expected)} scenarios matched across {len(results_by_sdk)} SDKs"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
