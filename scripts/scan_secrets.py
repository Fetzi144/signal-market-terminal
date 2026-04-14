from __future__ import annotations

import re
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
ALLOW_MARKER = "secret-scan: allow"
SKIP_SUFFIXES = {
    ".png",
    ".jpg",
    ".jpeg",
    ".gif",
    ".ico",
    ".pdf",
    ".woff",
    ".woff2",
    ".ttf",
    ".eot",
    ".zip",
    ".gz",
    ".tgz",
    ".db",
    ".sqlite",
    ".sqlite3",
    ".pyc",
}
SKIP_NAMES = {
    "package-lock.json",
}
PLACEHOLDER_SUBSTRINGS = {
    "example",
    "placeholder",
    "changeme",
    "replace_me",
    "replace-with",
    "dummy",
    "sample",
    "your_",
    "your-",
    "<",
    ">",
    "${",
    "localhost",
    "127.0.0.1",
    "test",
}
EXACT_PATTERNS = (
    ("private_key", re.compile(r"-----BEGIN [A-Z0-9 ]*PRIVATE KEY-----")),
    ("aws_access_key", re.compile(r"\b(?:A3T[A-Z0-9]|AKIA|ASIA)[A-Z0-9]{16}\b")),
    ("github_token", re.compile(r"\bgh[pousr]_[A-Za-z0-9]{20,255}\b")),
    ("slack_token", re.compile(r"\bxox[baprs]-[0-9A-Za-z-]{10,}\b")),
    ("openai_key", re.compile(r"\bsk-(?:proj-|live-|test-)?[A-Za-z0-9_-]{20,}\b")),
    ("stripe_live_key", re.compile(r"\b(?:sk|rk)_live_[0-9A-Za-z]{16,}\b")),
)
ASSIGNMENT_PATTERN = re.compile(
    r"^\s*-?\s*(?P<name>[A-Za-z0-9_]*(?:SECRET|TOKEN|PASSWORD|PRIVATE_KEY|ACCESS_KEY|API_KEY)[A-Za-z0-9_]*)"
    r"\s*[:=]\s*"
    r"(?P<quote>['\"]?)(?P<value>[^'\"\s#,;]+)(?P=quote)"
)


def _tracked_files() -> list[Path]:
    try:
        result = subprocess.run(
            ["git", "ls-files"],
            cwd=ROOT,
            check=True,
            capture_output=True,
            text=True,
        )
    except (OSError, subprocess.CalledProcessError):
        return [path for path in ROOT.rglob("*") if path.is_file() and ".git" not in path.parts]
    paths: list[Path] = []
    for raw_line in result.stdout.splitlines():
        candidate = (ROOT / raw_line).resolve()
        if candidate.is_file():
            paths.append(candidate)
    return paths


def _should_skip(path: Path) -> bool:
    if path.resolve() == Path(__file__).resolve():
        return True
    if path.name in SKIP_NAMES:
        return True
    if path.suffix.lower() in SKIP_SUFFIXES:
        return True
    return False


def _looks_like_placeholder(value: str, *, name: str) -> bool:
    lowered = value.lower()
    if not value or len(value) < 12:
        return True
    if any(token in lowered for token in PLACEHOLDER_SUBSTRINGS):
        return True
    if lowered in {"null", "none", "false", "true"}:
        return True
    if name.startswith("VITE_") or name.startswith("NEXT_PUBLIC_"):
        return True
    if value.count("*") >= 4:
        return True
    return False


def _scan_text(path: Path) -> list[tuple[str, int, str]]:
    findings: list[tuple[str, int, str]] = []
    try:
        text = path.read_text(encoding="utf-8")
    except UnicodeDecodeError:
        return findings
    if "\x00" in text:
        return findings

    for line_no, line in enumerate(text.splitlines(), start=1):
        if ALLOW_MARKER in line:
            continue
        for rule_name, pattern in EXACT_PATTERNS:
            if pattern.search(line):
                findings.append((rule_name, line_no, line.strip()))
        for match in ASSIGNMENT_PATTERN.finditer(line):
            name = match.group("name")
            value = match.group("value")
            if _looks_like_placeholder(value, name=name):
                continue
            findings.append(("sensitive_assignment", line_no, f"{name}=<redacted>"))
    return findings


def main() -> int:
    findings_by_file: list[tuple[Path, list[tuple[str, int, str]]]] = []
    scanned_count = 0
    for path in _tracked_files():
        if _should_skip(path):
            continue
        findings = _scan_text(path)
        scanned_count += 1
        if findings:
            findings_by_file.append((path, findings))

    if findings_by_file:
        print("Secret scan found potential secrets:")
        for path, findings in findings_by_file:
            relative_path = path.relative_to(ROOT)
            for rule_name, line_no, preview in findings:
                print(f"- {relative_path}:{line_no} [{rule_name}] {preview}")
        return 1

    print(f"Secret scan passed for {scanned_count} tracked text files.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
