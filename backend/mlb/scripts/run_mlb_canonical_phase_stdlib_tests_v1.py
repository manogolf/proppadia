#!/usr/bin/env python3
"""Execute the canonical-phase pytest-style tests with only the stdlib.

The repository's canonical interpreter intentionally has no pytest package.
This harness supplies only the two APIs used by the target module
(`mark.parametrize` and `raises`), then invokes the actual test functions and
their actual assertions.  It is not a substitute for general pytest.
"""

from __future__ import annotations

import argparse
import importlib
import inspect
import json
import re
import sys
import tempfile
import traceback
import types
from contextlib import AbstractContextManager
from pathlib import Path
from typing import Any, Callable

from backend.mlb.scripts.validate_mlb_canonical_game_phase_activation_v1 import (
    validate as validate_existing_checks,
)


CONTRACT_NAME = "MLB_2026_CANONICAL_PHASE_COVERAGE_AND_TEST_GATE_V1"
TARGET_MODULE = "backend.mlb.tests.test_mlb_canonical_game_phase_activation_v1"


class _Raises(AbstractContextManager[None]):
    def __init__(self, expected: type[BaseException], match: str | None = None) -> None:
        self.expected = expected
        self.match = match

    def __enter__(self) -> None:
        return None

    def __exit__(self, exc_type: Any, exc: BaseException | None, _tb: Any) -> bool:
        if exc_type is None:
            raise AssertionError(f"DID_NOT_RAISE:{self.expected.__name__}")
        if not issubclass(exc_type, self.expected):
            return False
        if self.match is not None and re.search(self.match, str(exc)) is None:
            raise AssertionError(
                f"EXCEPTION_MESSAGE_MISMATCH:{self.match!r}:{str(exc)!r}"
            )
        return True


class _Mark:
    @staticmethod
    def parametrize(
        argnames: str | tuple[str, ...], argvalues: list[Any]
    ) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
        names = (argnames,) if isinstance(argnames, str) else tuple(argnames)

        def decorate(function: Callable[..., Any]) -> Callable[..., Any]:
            cases: list[dict[str, Any]] = []
            for values in argvalues:
                row = (values,) if len(names) == 1 else tuple(values)
                if len(row) != len(names):
                    raise ValueError(f"PARAMETER_ARITY_MISMATCH:{function.__name__}")
                cases.append(dict(zip(names, row)))
            setattr(function, "_stdlib_parametrize", cases)
            return function

        return decorate


def _install_pytest_subset() -> None:
    module = types.ModuleType("pytest")
    module.mark = _Mark()  # type: ignore[attr-defined]
    module.raises = lambda expected, match=None: _Raises(expected, match)  # type: ignore[attr-defined]
    sys.modules["pytest"] = module


def _case_label(function_name: str, ordinal: int, parameters: dict[str, Any]) -> str:
    if not parameters:
        return function_name
    encoded = ",".join(f"{key}={value!r}" for key, value in parameters.items())
    return f"{function_name}[{ordinal}:{encoded}]"


def execute_target() -> dict[str, Any]:
    _install_pytest_subset()
    module = importlib.import_module(TARGET_MODULE)
    results: list[dict[str, Any]] = []
    functions = [
        value
        for name, value in vars(module).items()
        if name.startswith("test_") and inspect.isfunction(value)
    ]
    for function in functions:
        parameter_cases = getattr(function, "_stdlib_parametrize", None)
        cases = parameter_cases if parameter_cases is not None else [{}]
        for ordinal, parameters in enumerate(cases, 1):
            label = _case_label(function.__name__, ordinal, parameters)
            kwargs = dict(parameters)
            try:
                signature = inspect.signature(function)
                unexpected = set(kwargs) - set(signature.parameters)
                if unexpected:
                    raise AssertionError(
                        f"UNEXPECTED_PARAMETERS:{label}:{','.join(sorted(unexpected))}"
                    )
                if "tmp_path" in signature.parameters and "tmp_path" not in kwargs:
                    with tempfile.TemporaryDirectory(prefix="mlb_phase_test_") as temp_name:
                        kwargs["tmp_path"] = Path(temp_name)
                        function(**kwargs)
                else:
                    missing = set(signature.parameters) - set(kwargs)
                    if missing:
                        raise AssertionError(
                            f"UNSUPPORTED_FIXTURES:{label}:{','.join(sorted(missing))}"
                        )
                    function(**kwargs)
            except Exception as exc:
                results.append(
                    {
                        "scenario": label,
                        "test_function": function.__name__,
                        "status": "FAILED",
                        "error_type": type(exc).__name__,
                        "error": str(exc),
                        "traceback": traceback.format_exc(),
                    }
                )
            else:
                results.append(
                    {
                        "scenario": label,
                        "test_function": function.__name__,
                        "status": "PASSED",
                    }
                )

    passed = sum(row["status"] == "PASSED" for row in results)
    failed = sum(row["status"] == "FAILED" for row in results)
    skipped = sum(row["status"] == "SKIPPED" for row in results)
    validator = validate_existing_checks()
    coverage_map = {
        "all_postseason_rounds": ["test_all_postseason_rounds"],
        "regular_after_nominal_close": ["test_regular_after_nominal_close_remains_regular"],
        "postponed_rescheduled": ["test_postponed_rescheduled_relationships_are_exact"],
        "suspended_resumed": ["test_suspended_resumed_relationships_are_exact"],
        "all_star_and_special_exclusion": ["test_special_games_are_preserved_but_excluded"],
        "missing_unknown_conflicting_types": [
            "test_missing_or_nonexact_unknown_type_fails_closed",
            "test_conflicting_schedule_and_feed_types_fail_closed",
        ],
        "duplicate_game_pk_conflict": ["test_duplicate_game_pk_conflict_fails_closed"],
        "exact_game_pk_join": ["test_exact_game_pk_join_has_no_date_or_team_fallback"],
        "positional_insert_regression": [
            "test_cleanroom_insert_is_named_and_partial_schema_fails",
            "test_no_game_producer_uses_positional_insert",
        ],
        "zero_date_based_reconstruction": ["test_zero_date_based_phase_reconstruction"],
        "prepared_schema_and_exact_join_view": [
            "test_prepared_migration_persists_both_tables_and_join_is_exact"
        ],
        "offline_source_hashed_proposal": [
            "test_offline_builder_is_deterministic_and_source_hashed"
        ],
        "two_state_propagation_audit": [
            "test_propagation_audit_distinguishes_current_and_post_activation"
        ],
    }
    represented = {function.__name__ for function in functions}
    validator_checks = set(validator["checks"])
    missing_validator_mappings = sorted(validator_checks - set(coverage_map))
    missing_test_functions = sorted(
        name for names in coverage_map.values() for name in names if name not in represented
    )
    intended = len(results)
    executed = passed + failed
    gate_passed = (
        failed == 0
        and skipped == 0
        and executed == intended
        and validator.get("status") == "PASS"
        and not missing_validator_mappings
        and not missing_test_functions
    )
    return {
        "contract_name": CONTRACT_NAME,
        "target_module": TARGET_MODULE,
        "runner": "DEPENDENCY_FREE_STDLIB_ACTUAL_ASSERTION_EXECUTION",
        "intended_scenarios": intended,
        "executed_scenarios": executed,
        "passed": passed,
        "failed": failed,
        "skipped": skipped,
        "unexecuted": intended - executed,
        "existing_validator_check_count": validator.get("check_count"),
        "existing_validator_status": validator.get("status"),
        "validator_coverage_map": coverage_map,
        "missing_validator_mappings": missing_validator_mappings,
        "missing_test_functions": missing_test_functions,
        "gate_status": "PASS" if gate_passed else "FAIL",
        "results": results,
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    report = execute_target()
    rendered = json.dumps(report, sort_keys=True, indent=2) + "\n"
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(rendered, encoding="utf-8")
    print(rendered, end="")
    return 0 if report["gate_status"] == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
