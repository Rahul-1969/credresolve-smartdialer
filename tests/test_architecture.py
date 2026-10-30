"""Architecture enforcement tests verifying modular isolation and zero-override safety invariants."""

import ast
from dataclasses import FrozenInstanceError, is_dataclass
from pathlib import Path
import sys
import pytest

from smartdialer.safety import SafetyConfig

FORBIDDEN_PACING_MODULES = {
    "sqlite3",
    "smartdialer.safety",
    "smartdialer.providers",
    "smartdialer.allocator",
    "smartdialer.dispatch",
    "smartdialer.repo",
    "smartdialer.db",
    "smartdialer.metrics",
}

ALLOWED_SMARTDIALER_ROOTS = {
    "smartdialer.pacing",
    "smartdialer.snapshot",
    "smartdialer.models",
}

FORBIDDEN_IDENTIFIERS = {"enabled", "disable", "disabled", "bypass", "skip"}


def get_imports_from_ast(tree: ast.AST) -> list[str]:
    """Extract all module names imported in an AST."""
    imports = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                imports.append(alias.name)
        elif isinstance(node, ast.ImportFrom):
            if node.module:
                imports.append(node.module)
    return imports


def validate_pacing_imports(source: str, filename: str = "<string>") -> None:
    """Validate that imports in a pacing module comply with architecture constraints."""
    tree = ast.parse(source, filename=filename)
    imports = get_imports_from_ast(tree)

    stdlib_modules = set(sys.stdlib_module_names)

    for mod in imports:
        # Check against forbidden modules directly
        for forbidden in FORBIDDEN_PACING_MODULES:
            if mod == forbidden or mod.startswith(forbidden + "."):
                raise AssertionError(f"Forbidden import '{mod}' found in {filename}")

        # If it's a smartdialer import, must be in allowed roots
        if mod.startswith("smartdialer."):
            is_allowed = any(
                mod == allowed or mod.startswith(allowed + ".")
                for allowed in ALLOWED_SMARTDIALER_ROOTS
            )
            if not is_allowed:
                raise AssertionError(f"Disallowed internal import '{mod}' found in {filename}")
        else:
            root = mod.split(".")[0]
            if root not in stdlib_modules:
                raise AssertionError(f"Non-stdlib external import '{mod}' found in {filename}")


def get_identifiers_from_ast(tree: ast.AST) -> set[str]:
    """Collect all defined or referenced names/identifiers in an AST."""
    names = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Name):
            names.add(node.id.lower())
        elif isinstance(node, ast.Attribute):
            names.add(node.attr.lower())
        elif isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            names.add(node.name.lower())
        elif isinstance(node, ast.arg):
            names.add(node.arg.lower())
    return names


def validate_safety_identifiers(source: str, filename: str = "<string>") -> None:
    """Validate that no bypass or enable/disable identifiers exist in safety code."""
    tree = ast.parse(source, filename=filename)
    identifiers = get_identifiers_from_ast(tree)
    found_forbidden = identifiers.intersection(FORBIDDEN_IDENTIFIERS)
    if found_forbidden:
        raise AssertionError(
            f"Forbidden safety bypass identifiers found in {filename}: {found_forbidden}"
        )


def test_pacing_modules_architecture_isolation():
    """Every .py under smartdialer/pacing/ must only import stdlib, snapshot, models, or pacing."""
    pacing_dir = Path("smartdialer/pacing")
    pacing_files = list(pacing_dir.glob("*.py"))
    assert len(pacing_files) >= 3, f"Expected pacing files, found: {pacing_files}"

    for py_file in pacing_files:
        source = py_file.read_text(encoding="utf-8")
        validate_pacing_imports(source, str(py_file))


def test_safety_contains_no_bypass_or_toggle_identifiers():
    """smartdialer/safety.py must contain no identifiers named enabled, disable, disabled, bypass, skip."""
    safety_file = Path("smartdialer/safety.py")
    assert safety_file.exists()
    source = safety_file.read_text(encoding="utf-8")
    validate_safety_identifiers(source, str(safety_file))


def test_safety_config_invariants():
    """SafetyConfig must have no bypass/enable fields and must be immutable (frozen)."""
    assert is_dataclass(SafetyConfig)

    # Check field names
    for field_name in SafetyConfig.__dataclass_fields__:
        lower = field_name.lower()
        for forbidden in FORBIDDEN_IDENTIFIERS:
            assert forbidden not in lower, f"SafetyConfig has forbidden field '{field_name}'"

    # Check frozenness
    cfg = SafetyConfig()
    with pytest.raises(FrozenInstanceError):
        cfg.max_overdial_ratio = 1.0  # type: ignore


def test_architecture_checker_fails_on_synthetic_violations():
    """Synthetic check: import validator detects violations and cannot pass vacuously."""
    # 1. Detect forbidden import
    synthetic_code = "import smartdialer.safety\n"
    with pytest.raises(AssertionError, match="Forbidden import 'smartdialer.safety'"):
        validate_pacing_imports(synthetic_code, "synthetic_fail.py")

    synthetic_db = "import sqlite3\n"
    with pytest.raises(AssertionError, match="Forbidden import 'sqlite3'"):
        validate_pacing_imports(synthetic_db, "synthetic_fail.py")

    # 2. Detect forbidden identifier
    synthetic_id = "def bypass(): pass\n"
    with pytest.raises(AssertionError, match="Forbidden safety bypass identifiers"):
        validate_safety_identifiers(synthetic_id, "synthetic_fail.py")

    synthetic_enable = "enabled = True\n"
    with pytest.raises(AssertionError, match="Forbidden safety bypass identifiers"):
        validate_safety_identifiers(synthetic_enable, "synthetic_fail.py")
