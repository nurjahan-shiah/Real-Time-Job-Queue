"""Tests for the Day 15 system design writeup, docs/system-design.md.

The writeup is documentation, so the tests check that it exists, that it
covers every major part of the system, that the numbers it quotes match
the code and the measured load test results, and that it follows the
project style rule, commas only, no em dash, no double hyphen.
"""
from pathlib import Path

from queue.jobs import (
    DEFAULT_BACKOFF_BASE_SECONDS,
    DEFAULT_BACKOFF_CAP_SECONDS,
    MAX_BATCH_SIZE,
    POISON_SAME_ERROR_THRESHOLD,
)

REPO_ROOT = Path(__file__).resolve().parents[1]
DOC_PATH = REPO_ROOT / "docs" / "system-design.md"
README_PATH = REPO_ROOT / "README.md"

EXPECTED_SECTIONS = [
    "## Goals",
    "## Non-goals",
    "## Architecture",
    "## Data model",
    "## API surface",
    "## Job lifecycle",
    "## Claiming and concurrency",
    "## Delayed jobs and priority",
    "## Retries and backoff",
    "## Timeouts",
    "## Poison jobs",
    "## Rate limiting",
    "## Progress and cancellation",
    "## Recovery on restart",
    "## Batch enqueue",
    "## Dashboard and observability",
    "## Measured performance",
    "## Limits of this design",
    "## What would change at larger scale",
    "## Failure modes",
    "## Alternatives considered",
]


def read_doc():
    assert DOC_PATH.exists(), "docs/system-design.md is missing"
    return DOC_PATH.read_text()


def test_doc_exists_and_is_substantial():
    text = read_doc()
    assert len(text.splitlines()) > 100


def test_doc_covers_every_major_part():
    text = read_doc()
    for section in EXPECTED_SECTIONS:
        assert section in text, f"missing section {section}"


def test_doc_quotes_measured_numbers():
    text = read_doc()
    assert "361" in text
    assert "399" in text
    assert "67" in text
    assert "30,000" in text
    assert "0.7" in text
    assert "500" in text
    assert "2026-11-20" in text


def test_doc_matches_code_constants():
    text = read_doc()
    assert str(MAX_BATCH_SIZE) in text
    assert "3 times in a row" in text
    assert POISON_SAME_ERROR_THRESHOLD == 3
    assert f"{DEFAULT_BACKOFF_BASE_SECONDS} seconds" in text
    assert f"{DEFAULT_BACKOFF_CAP_SECONDS} seconds" in text
    assert "priority descending" in text
    assert "WAL mode" in text
    assert "30 second busy timeout" in text


def test_doc_names_the_schema():
    text = read_doc()
    for table in ("jobs", "rate_limits", "rate_limit_runs", "job_failures"):
        assert table in text, f"missing table {table}"
    for column in ("idempotency_key", "claimed_by", "finished_at", "poison"):
        assert column in text, f"missing column {column}"


def test_doc_follows_style_rule():
    text = read_doc()
    assert "\u2014" not in text
    assert "--" not in text


def test_readme_points_at_the_writeup():
    assert README_PATH.exists(), "README.md is missing"
    text = README_PATH.read_text()
    assert "docs/system-design.md" in text
    assert "- [x] Day 15, System design writeup, docs/system-design.md" in text
