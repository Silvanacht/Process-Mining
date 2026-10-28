#!/usr/bin/env python3
"""Waiting-time analysis for activity-instance event logs.

Implements an end-to-end, configurable pipeline inspired by Lashkevich et al.
(2023), "Why Am I Waiting? Data-Driven Analysis of Waiting Times in Business
Processes". The program:

* validates CSV activity-instance logs;
* infers enabling predecessors and transition instances;
* decomposes each waiting interval, without double counting, into batching,
  resource contention, prioritization, resource unavailability, and extraneous;
* aggregates transition reports;
* calculates cycle-time efficiency (CTE);
* compares generated reports with supplied expected CSV files;
* evaluates LoanApp batching labels without using them as detector inputs;
* writes reproducibility and data-quality metadata.

Important: concurrency, batching, and calendar inference are configurable
heuristics. For strict paper replication, use the same parameter values as the
caise2023 reference implementation and verify all manual expected files.

Dependencies: pandas, numpy, PyYAML (optional; required only for YAML configs).
"""
from __future__ import annotations

import argparse
import hashlib
import json
import logging
import math
import os
import platform
import re
import sys
import time
from dataclasses import asdict, dataclass, field
from datetime import datetime, time as dtime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable, Optional, Sequence

import numpy as np
import pandas as pd

try:
    import yaml
except ImportError:  # YAML is optional when only CLI/defaults are used.
    yaml = None

VERSION = "1.0.0"
CAUSES = ("batching", "contention", "prioritization", "unavailability", "extraneous")
LOG = logging.getLogger("waiting_time_analysis")


# -----------------------------------------------------------------------------
# Configuration and data models
# -----------------------------------------------------------------------------
@dataclass
class AnalysisConfig:
    case_col: str = "case_id"
    activity_col: str = "Activity"
    start_col: str = "start_time"
    end_col: str = "end_time"
    resource_col: str = "Resource"
    timezone: str = "UTC"
    delimiter: str = ","
    encoding: str = "utf-8-sig"

    # Transition discovery. closest_non_concurrent is general-purpose;
    # adjacent is useful only for known sequential logs.
    transition_method: str = "closest_non_concurrent"
    concurrency_threshold: float = 0.1
    max_predecessors: int = 1
    include_zero_wait: bool = True

    # Batch discovery: same activity/resource, consecutive executions, and a
    # minimum group size. Start gaps up to this value are considered one batch.
    batch_min_size: int = 2
    batch_start_gap_seconds: float = 60.0
    batch_same_day: bool = True

    # Weekly resource calendar. Use schedule for deterministic/manual tests or
    # inferred for larger event logs. Working weekdays use Monday=0.
    calendar_method: str = "schedule"  # schedule | inferred | always
    working_weekdays: list[int] = field(default_factory=lambda: [0, 1, 2, 3, 4])
    work_start: str = "09:00:00"
    work_end: str = "17:00:00"
    break_start: Optional[str] = "12:00:00"
    break_end: Optional[str] = "12:30:00"
    calendar_slot_minutes: int = 15
    calendar_min_events: int = 1
    calendar_probability_threshold: float = 0.1

    # Evaluation and output.
    duration_tolerance_seconds: float = 1.0
    write_instances: bool = True
    cause_order: list[str] = field(default_factory=lambda: list(CAUSES))

    @classmethod
    def from_yaml(cls, path: Optional[str]) -> "AnalysisConfig":
        cfg = cls()
        if not path:
            return cfg
        with open(path, "r", encoding="utf-8") as fh:
            if yaml is not None:
                raw = yaml.safe_load(fh) or {}
            else:
                try:
                    raw = json.load(fh) or {}
                except Exception as exc:
                    raise RuntimeError("PyYAML is required for YAML --config. Install pyyaml.") from exc
        # Accept either a flat config or sections matching the README/master prompt.
        merged: dict[str, Any] = {}
        merged.update(raw)
        mapping = {
            "input": {"case_id_column": "case_col", "activity_column": "activity_col",
                      "start_time_column": "start_col", "end_time_column": "end_col",
                      "resource_column": "resource_col", "timezone": "timezone",
                      "delimiter": "delimiter", "encoding": "encoding"},
            "transition_discovery": {"method": "transition_method",
                                     "concurrency_threshold": "concurrency_threshold",
                                     "max_predecessors": "max_predecessors"},
            "batching": {"minimum_batch_size": "batch_min_size",
                         "start_gap_seconds": "batch_start_gap_seconds",
                         "same_day": "batch_same_day"},
            "calendar": {"method": "calendar_method", "working_days": "working_weekdays",
                         "working_start": "work_start", "working_end": "work_end",
                         "break_start": "break_start", "break_end": "break_end",
                         "slot_minutes": "calendar_slot_minutes",
                         "minimum_observations": "calendar_min_events",
                         "probability_threshold": "calendar_probability_threshold"},
            "evaluation": {"duration_tolerance_seconds": "duration_tolerance_seconds"},
            "output": {"write_instance_report": "write_instances"},
        }
        for section, keys in mapping.items():
            for old, new in keys.items():
                if isinstance(raw.get(section), dict) and old in raw[section]:
                    merged[new] = raw[section][old]
        valid = set(cls.__dataclass_fields__)
        unknown = sorted(k for k in merged if k not in valid and k not in mapping)
        for key in unknown:
            if not isinstance(merged[key], dict):
                LOG.warning("Ignoring unknown configuration key: %s", key)
        values = {k: v for k, v in merged.items() if k in valid}
        return cls(**values)

    def validate(self) -> None:
        if self.transition_method not in ("adjacent", "closest_non_concurrent"):
            raise ValueError("transition_method must be adjacent or closest_non_concurrent")
        if self.calendar_method not in ("schedule", "inferred", "always"):
            raise ValueError("calendar_method must be schedule, inferred, or always")
        if sorted(self.cause_order) != sorted(CAUSES):
            raise ValueError(f"cause_order must contain exactly {CAUSES}")
        if self.batch_min_size < 2:
            raise ValueError("batch_min_size must be >= 2")
        if self.calendar_slot_minutes <= 0 or 1440 % self.calendar_slot_minutes:
            raise ValueError("calendar_slot_minutes must divide 1440")


@dataclass(frozen=True, order=True)
class Interval:
    start: pd.Timestamp
    end: pd.Timestamp

    def __post_init__(self) -> None:
        if pd.isna(self.start) or pd.isna(self.end):
            raise ValueError("Interval endpoints cannot be null")
        if self.end < self.start:
            raise ValueError(f"Invalid interval: {self.start} > {self.end}")

    @property
    def seconds(self) -> float:
        return max(0.0, (self.end - self.start).total_seconds())

    def intersection(self, other: "Interval") -> Optional["Interval"]:
        left, right = max(self.start, other.start), min(self.end, other.end)
        return Interval(left, right) if right > left else None


# -----------------------------------------------------------------------------
# Generic utilities
# -----------------------------------------------------------------------------
def file_sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as fh:
        for block in iter(lambda: fh.read(1024 * 1024), b""):
            h.update(block)
    return h.hexdigest()

def iso_now() -> str:
    return datetime.now(timezone.utc).isoformat()

def format_timedelta(seconds: float) -> str:
    return str(pd.to_timedelta(round(float(seconds), 6), unit="s"))

def merge_intervals(intervals: Iterable[Interval], merge_touching: bool = True) -> list[Interval]:
    values = sorted((x for x in intervals if x.seconds > 0), key=lambda x: (x.start, x.end))
    if not values:
        return []
    result = [values[0]]
    for cur in values[1:]:
        prev = result[-1]
        overlaps = cur.start <= prev.end if merge_touching else cur.start < prev.end
        if overlaps:
            result[-1] = Interval(prev.start, max(prev.end, cur.end))
        else:
            result.append(cur)
    return result

def clip_intervals(intervals: Iterable[Interval], parent: Interval) -> list[Interval]:
    return merge_intervals(x for x in (i.intersection(parent) for i in intervals) if x is not None)

def subtract_intervals(base: Interval, removals: Iterable[Interval]) -> list[Interval]:
    remaining = [base]
    for rem in clip_intervals(removals, base):
        next_parts: list[Interval] = []
        for part in remaining:
            overlap = part.intersection(rem)
            if overlap is None:
                next_parts.append(part)
                continue
            if part.start < overlap.start:
                next_parts.append(Interval(part.start, overlap.start))
            if overlap.end < part.end:
                next_parts.append(Interval(overlap.end, part.end))
        remaining = next_parts
    return merge_intervals(remaining)

def intersect_many(candidates: Iterable[Interval], allowed: Iterable[Interval]) -> list[Interval]:
    out: list[Interval] = []
    for a in candidates:
        for b in allowed:
            hit = a.intersection(b)
            if hit:
                out.append(hit)
    return merge_intervals(out)

def total_seconds(intervals: Iterable[Interval]) -> float:
    return float(sum(i.seconds for i in merge_intervals(intervals)))

def parse_clock(value: str) -> dtime:
    return datetime.strptime(value, "%H:%M:%S").time()

def safe_div(num: float, den: float) -> float:
    return float(num / den) if den else 0.0


# -----------------------------------------------------------------------------
# Loading and validation
# -----------------------------------------------------------------------------
class EventLogLoader:
    def __init__(self, config: AnalysisConfig):
        self.cfg = config

    def load(self, path: Path) -> tuple[pd.DataFrame, dict[str, Any]]:
        df = pd.read_csv(path, sep=self.cfg.delimiter, encoding=self.cfg.encoding)
        required = [self.cfg.case_col, self.cfg.activity_col, self.cfg.start_col,
                    self.cfg.end_col, self.cfg.resource_col]
        missing = [c for c in required if c not in df.columns]
        if missing:
            raise ValueError(f"Missing required columns: {missing}; found {list(df.columns)}")

        original_columns = list(df.columns)
        rename = {self.cfg.case_col: "case_id", self.cfg.activity_col: "activity",
                  self.cfg.start_col: "start_time", self.cfg.end_col: "end_time",
                  self.cfg.resource_col: "resource"}
        df = df.rename(columns=rename).copy()
        df["source_row"] = np.arange(len(df), dtype=int)
        for col in ("start_time", "end_time"):
            # format='mixed' is available in modern pandas; fallback supports older versions.
            try:
                parsed = pd.to_datetime(df[col], errors="coerce", utc=True, format="mixed")
            except TypeError:
                parsed = pd.to_datetime(df[col], errors="coerce", utc=True)
            df[col] = parsed

        quality = self.validate(df)
        df["case_id"] = df["case_id"].astype(str)
        df["activity"] = df["activity"].astype(str).str.strip()
        df["resource"] = df["resource"].astype(str).str.strip()
        df["event_id"] = [f"e{i}" for i in df["source_row"]]
        df["processing_seconds"] = (df["end_time"] - df["start_time"]).dt.total_seconds()
        df = df.sort_values(["case_id", "start_time", "end_time", "source_row"], kind="stable")
        df = df.reset_index(drop=True)
        quality["original_columns"] = original_columns
        quality["normalized_columns"] = list(df.columns)
        return df, quality

    def validate(self, df: pd.DataFrame) -> dict[str, Any]:
        required_normalized = ["case_id", "activity", "start_time", "end_time", "resource"]
        nulls = {c: int(df[c].isna().sum()) for c in required_normalized}
        parse_failures = int(df["start_time"].isna().sum() + df["end_time"].isna().sum())
        if parse_failures:
            examples = df[df["start_time"].isna() | df["end_time"].isna()].head(5).to_dict("records")
            raise ValueError(f"Unparseable timestamps ({parse_failures}); examples: {examples}")
        for col in ("case_id", "activity", "resource"):
            if df[col].isna().any():
                raise ValueError(f"Column {col} contains nulls")
        negative = df["end_time"] < df["start_time"]
        if negative.any():
            raise ValueError(f"Found {int(negative.sum())} events ending before they start")
        logical_cols = ["case_id", "activity", "start_time", "end_time", "resource"]
        same_resource_overlap = 0
        for _, g in df.sort_values("start_time").groupby("resource", dropna=False):
            prior_end: Optional[pd.Timestamp] = None
            for row in g.itertuples():
                if prior_end is not None and row.start_time < prior_end:
                    same_resource_overlap += 1
                prior_end = max(prior_end, row.end_time) if prior_end is not None else row.end_time
        return {
            "rows": int(len(df)),
            "cases": int(df["case_id"].nunique(dropna=True)),
            "activities": int(df["activity"].nunique(dropna=True)),
            "resources": int(df["resource"].nunique(dropna=True)),
            "null_counts": nulls,
            "zero_duration_events": int((df["start_time"] == df["end_time"]).sum()),
            "exact_duplicate_rows": int(df.duplicated().sum()),
            "logical_duplicate_rows": int(df.duplicated(logical_cols).sum()),
            "same_resource_overlap_observations": int(same_resource_overlap),
            "single_event_cases": int((df.groupby("case_id").size() == 1).sum()),
        }


# -----------------------------------------------------------------------------
# Transition discovery
# -----------------------------------------------------------------------------
class ConcurrencyOracle:
    """Activity-pair concurrency inferred from bidirectional temporal order.

    Activities A and B are considered concurrent when both A-before-B and
    B-before-A relations occur often enough across cases. This is a compact,
    deterministic approximation of a Heuristics-Miner-style concurrency oracle.
    """
    def __init__(self, threshold: float):
        self.threshold = threshold
        self.concurrent: set[frozenset[str]] = set()

    def fit(self, events: pd.DataFrame) -> "ConcurrencyOracle":
        counts: dict[tuple[str, str], int] = {}
        for _, g in events.groupby("case_id", sort=False):
            rows = list(g.sort_values(["start_time", "end_time"]).itertuples())
            for i, a in enumerate(rows):
                for b in rows[i + 1:]:
                    if a.end_time <= b.start_time:
                        counts[(a.activity, b.activity)] = counts.get((a.activity, b.activity), 0) + 1
                    elif b.end_time <= a.start_time:
                        counts[(b.activity, a.activity)] = counts.get((b.activity, a.activity), 0) + 1
                    elif a.activity != b.activity:
                        self.concurrent.add(frozenset((a.activity, b.activity)))
        pairs = {frozenset(k) for k in counts if k[0] != k[1]}
        for pair in pairs:
            a, b = tuple(pair)
            ab, ba = counts.get((a, b), 0), counts.get((b, a), 0)
            den = ab + ba
            if den and min(ab, ba) / den >= self.threshold:
                self.concurrent.add(pair)
        return self

    def are_concurrent(self, a: str, b: str) -> bool:
        return a != b and frozenset((a, b)) in self.concurrent


class TransitionDiscoverer:
    def __init__(self, config: AnalysisConfig):
        self.cfg = config

    def discover(self, events: pd.DataFrame) -> pd.DataFrame:
        oracle = ConcurrencyOracle(self.cfg.concurrency_threshold).fit(events)
        records: list[dict[str, Any]] = []
        for case_id, group in events.groupby("case_id", sort=False):
            g = group.sort_values(["start_time", "end_time", "source_row"], kind="stable")
            rows = list(g.itertuples())
            for target_pos, target in enumerate(rows):
                predecessors = self._predecessors(rows, target_pos, oracle)
                for source in predecessors:
                    enabled = source.end_time
                    wait_seconds = max(0.0, (target.start_time - enabled).total_seconds())
                    if wait_seconds == 0 and not self.cfg.include_zero_wait:
                        continue
                    records.append({
                        "case_id": str(case_id),
                        "source_event_id": source.event_id,
                        "target_event_id": target.event_id,
                        "source_activity": source.activity,
                        "source_resource": source.resource,
                        "target_activity": target.activity,
                        "target_resource": target.resource,
                        "source_start": source.start_time,
                        "source_end": source.end_time,
                        "enabled_time": enabled,
                        "target_start": target.start_time,
                        "target_end": target.end_time,
                        "wt_total_seconds": wait_seconds,
                    })
        columns = ["case_id", "source_event_id", "target_event_id", "source_activity",
                   "source_resource", "target_activity", "target_resource", "source_start",
                   "source_end", "enabled_time", "target_start", "target_end", "wt_total_seconds"]
        return pd.DataFrame.from_records(records, columns=columns)

    def _predecessors(self, rows: list[Any], pos: int, oracle: ConcurrencyOracle) -> list[Any]:
        if pos == 0:
            return []
        target = rows[pos]
        prior = rows[:pos]
        if self.cfg.transition_method == "adjacent":
            return [prior[-1]]
        candidates = [r for r in prior if r.end_time <= target.start_time and
                      not oracle.are_concurrent(r.activity, target.activity)]
        if not candidates:
            return []
        latest_end = max(r.end_time for r in candidates)
        latest = [r for r in candidates if r.end_time == latest_end]
        latest.sort(key=lambda r: (r.start_time, getattr(r, "source_row", getattr(r, "Index", 0))), reverse=True)
        return latest[: self.cfg.max_predecessors]


# -----------------------------------------------------------------------------
# Batch detection and calendars
# -----------------------------------------------------------------------------
class BatchDetector:
    def __init__(self, config: AnalysisConfig):
        self.cfg = config

    def detect(self, events: pd.DataFrame, transitions: pd.DataFrame) -> tuple[dict[str, str], dict[str, list[str]]]:
        enabled = transitions.drop_duplicates("target_event_id").set_index("target_event_id")["enabled_time"].to_dict()
        target_rows = events[events["event_id"].isin(enabled)].copy()
        target_rows["enabled_time"] = target_rows["event_id"].map(enabled)
        assignment: dict[str, str] = {}
        groups: dict[str, list[str]] = {}
        batch_no = 0
        for (_, _), g in target_rows.groupby(["activity", "resource"], sort=False):
            g = g.sort_values(["start_time", "end_time"])
            current: list[Any] = []
            prior_end: Optional[pd.Timestamp] = None
            prior_start: Optional[pd.Timestamp] = None
            for row in g.itertuples():
                same_day = True
                if self.cfg.batch_same_day and prior_start is not None:
                    same_day = row.start_time.date() == prior_start.date()
                gap = math.inf if prior_end is None else (row.start_time - prior_end).total_seconds()
                consecutive = prior_end is not None and gap <= self.cfg.batch_start_gap_seconds and gap >= 0
                if current and not (same_day and consecutive):
                    if len(current) >= self.cfg.batch_min_size:
                        batch_no = self._save(current, batch_no, assignment, groups)
                    current = []
                current.append(row)
                prior_end, prior_start = row.end_time, row.start_time
            if len(current) >= self.cfg.batch_min_size:
                batch_no = self._save(current, batch_no, assignment, groups)
        return assignment, groups

    @staticmethod
    def _save(rows: list[Any], number: int, assignment: dict[str, str], groups: dict[str, list[str]]) -> int:
        number += 1
        bid = f"predicted_batch_{number}"
        ids = [r.event_id for r in rows]
        groups[bid] = ids
        assignment.update({eid: bid for eid in ids})
        return number


class ResourceCalendar:
    def __init__(self, config: AnalysisConfig, events: pd.DataFrame):
        self.cfg = config
        self.events = events
        self.start_clock = parse_clock(config.work_start)
        self.end_clock = parse_clock(config.work_end)
        self.inferred: dict[str, set[tuple[int, int]]] = {}
        if config.calendar_method == "inferred":
            self._infer()

    def _infer(self) -> None:
        slot = self.cfg.calendar_slot_minutes
        for resource, group in self.events.groupby("resource"):
            occupied: dict[tuple[int, int], int] = {}
            weeks = max(1, int(math.ceil((group["end_time"].max() - group["start_time"].min()).days / 7)))
            for row in group.itertuples():
                cur = row.start_time.floor(f"{slot}min")
                while cur < row.end_time:
                    key = (cur.weekday(), (cur.hour * 60 + cur.minute) // slot)
                    occupied[key] = occupied.get(key, 0) + 1
                    cur += pd.Timedelta(minutes=slot)
            if len(group) < self.cfg.calendar_min_events:
                LOG.warning("Too few events to infer calendar for resource %s; using schedule fallback", resource)
                continue
            self.inferred[str(resource)] = {k for k, n in occupied.items()
                                            if n / weeks >= self.cfg.calendar_probability_threshold}

    def unavailable(self, resource: str, interval: Interval) -> list[Interval]:
        if interval.seconds <= 0 or self.cfg.calendar_method == "always":
            return []
        if self.cfg.calendar_method == "inferred" and resource in self.inferred:
            return self._unavailable_inferred(resource, interval)
        return self._unavailable_schedule(interval)

    def _unavailable_inferred(self, resource: str, interval: Interval) -> list[Interval]:
        slot = self.cfg.calendar_slot_minutes
        cur = interval.start.floor(f"{slot}min")
        out: list[Interval] = []
        allowed = self.inferred[resource]
        while cur < interval.end:
            nxt = cur + pd.Timedelta(minutes=slot)
            key = (cur.weekday(), (cur.hour * 60 + cur.minute) // slot)
            if key not in allowed:
                hit = Interval(cur, nxt).intersection(interval)
                if hit:
                    out.append(hit)
            cur = nxt
        return merge_intervals(out)

    def _unavailable_schedule(self, interval: Interval) -> list[Interval]:
        available: list[Interval] = []
        day = interval.start.normalize()
        last_day = interval.end.normalize()
        b_start = parse_clock(self.cfg.break_start) if self.cfg.break_start else None
        b_end = parse_clock(self.cfg.break_end) if self.cfg.break_end else None
        while day <= last_day:
            if day.weekday() in self.cfg.working_weekdays:
                left = pd.Timestamp.combine(day.date(), self.start_clock).tz_localize(interval.start.tz)
                right = pd.Timestamp.combine(day.date(), self.end_clock).tz_localize(interval.start.tz)
                day_avail = [Interval(left, right)]
                if b_start and b_end:
                    bl = pd.Timestamp.combine(day.date(), b_start).tz_localize(interval.start.tz)
                    br = pd.Timestamp.combine(day.date(), b_end).tz_localize(interval.start.tz)
                    day_avail = subtract_intervals(Interval(left, right), [Interval(bl, br)])
                for slot_iv in day_avail:
                    hit = slot_iv.intersection(interval)
                    if hit:
                        available.append(hit)
            day += pd.Timedelta(days=1)
        return subtract_intervals(interval, available)


# -----------------------------------------------------------------------------
# Cause decomposition
# -----------------------------------------------------------------------------
class WaitingTimeAnalyzer:
    def __init__(self, config: AnalysisConfig):
        self.cfg = config

    def analyze(self, events: pd.DataFrame, transitions: pd.DataFrame) -> tuple[pd.DataFrame, dict[str, list[str]]]:
        if transitions.empty:
            return transitions.copy(), {}
        batch_assignment, batch_groups = BatchDetector(self.cfg).detect(events, transitions)
        calendar = ResourceCalendar(self.cfg, events)
        enabled_map = transitions.drop_duplicates("target_event_id").set_index("target_event_id")["enabled_time"].to_dict()
        event_by_id = events.set_index("event_id", drop=False)
        by_resource = {str(r): g.sort_values("start_time") for r, g in events.groupby("resource")}
        out: list[dict[str, Any]] = []

        for tr in transitions.itertuples(index=False):
            full = Interval(tr.enabled_time, tr.target_start)
            remaining = [full] if full.seconds > 0 else []
            cause_intervals: dict[str, list[Interval]] = {c: [] for c in CAUSES}

            for cause in self.cfg.cause_order:
                if not remaining:
                    break
                if cause == "batching":
                    candidates = self._batch_intervals(tr, batch_assignment, batch_groups, event_by_id, enabled_map)
                elif cause in ("contention", "prioritization"):
                    candidates = self._resource_intervals(tr, cause, by_resource, enabled_map)
                elif cause == "unavailability":
                    candidates = calendar.unavailable(str(tr.target_resource), full)
                else:
                    candidates = remaining
                accepted = intersect_many(candidates, remaining)
                cause_intervals[cause] = accepted
                for accepted_interval in accepted:
                    next_remaining: list[Interval] = []
                    for rem in remaining:
                        next_remaining.extend(subtract_intervals(rem, [accepted_interval]))
                    remaining = merge_intervals(next_remaining)

            # Extraneous is always any residual, even if cause order was altered.
            if remaining:
                cause_intervals["extraneous"] = merge_intervals(cause_intervals["extraneous"] + remaining)
                remaining = []
            rec = tr._asdict()
            for cause in CAUSES:
                rec[f"wt_{cause}_seconds"] = total_seconds(cause_intervals[cause])
                rec[f"{cause}_intervals"] = self._serialize_intervals(cause_intervals[cause])
            assigned = sum(rec[f"wt_{c}_seconds"] for c in CAUSES)
            rec["conservation_error_seconds"] = rec["wt_total_seconds"] - assigned
            if abs(rec["conservation_error_seconds"]) > self.cfg.duration_tolerance_seconds:
                raise AssertionError(f"Cause-duration conservation failed for {tr.target_event_id}: {rec}")
            rec["predicted_batch_id"] = batch_assignment.get(tr.target_event_id, "")
            out.append(rec)
        return pd.DataFrame(out), batch_groups

    @staticmethod
    def _serialize_intervals(intervals: list[Interval]) -> str:
        return json.dumps([[i.start.isoformat(), i.end.isoformat()] for i in intervals])

    def _batch_intervals(self, tr: Any, assignment: dict[str, str], groups: dict[str, list[str]],
                         event_by_id: pd.DataFrame, enabled: dict[str, pd.Timestamp]) -> list[Interval]:
        bid = assignment.get(tr.target_event_id)
        if not bid:
            return []
        members = groups[bid]
        member_enabled = [enabled[e] for e in members if e in enabled]
        if not member_enabled:
            return []
        accumulation_end = max(member_enabled)
        if accumulation_end <= tr.enabled_time:
            return []
        return [Interval(tr.enabled_time, min(accumulation_end, tr.target_start))]

    @staticmethod
    def _resource_intervals(tr: Any, cause: str, by_resource: dict[str, pd.DataFrame],
                            enabled: dict[str, pd.Timestamp]) -> list[Interval]:
        waiting = Interval(tr.enabled_time, tr.target_start)
        out: list[Interval] = []
        for other in by_resource.get(str(tr.target_resource), pd.DataFrame()).itertuples():
            if other.event_id == tr.target_event_id:
                continue
            executed = Interval(other.start_time, other.end_time)
            overlap = waiting.intersection(executed)
            if overlap is None:
                continue
            other_enabled = enabled.get(other.event_id)
            if other_enabled is None:
                # Initial case events have no preceding transition; in Lashkevich et al. (2023),
                # overlapping work from initial case events causes resource contention.
                if cause == "contention":
                    out.append(overlap)
                continue
            if cause == "contention" and other_enabled <= tr.enabled_time:
                out.append(overlap)
            elif cause == "prioritization" and other_enabled > tr.enabled_time:
                out.append(overlap)
        return merge_intervals(out)


# -----------------------------------------------------------------------------
# Aggregation, CTE, and evaluation
# -----------------------------------------------------------------------------
class TransitionAggregator:
    KEYS = ["source_activity", "source_resource", "target_activity", "target_resource"]

    def aggregate(self, instances: pd.DataFrame) -> pd.DataFrame:
        columns = self.KEYS + ["frequency", "cases", "wt_total", "wt_batching",
                               "wt_prioritization", "wt_contention", "wt_unavailability",
                               "wt_extraneous", "wt_total_seconds"]
        if instances.empty:
            return pd.DataFrame(columns=columns)
        rows: list[dict[str, Any]] = []
        for keys, g in instances.groupby(self.KEYS, dropna=False, sort=True):
            rec = dict(zip(self.KEYS, keys))
            rec["frequency"] = float(len(g))
            rec["cases"] = ",".join(sorted(g["case_id"].astype(str).unique(), key=self._natural_key))
            rec["wt_total_seconds"] = float(g["wt_total_seconds"].sum())
            rec["wt_total"] = format_timedelta(rec["wt_total_seconds"])
            for cause in CAUSES:
                sec = float(g[f"wt_{cause}_seconds"].sum())
                rec[f"wt_{cause}"] = format_timedelta(sec)
                rec[f"wt_{cause}_seconds"] = sec
            rows.append(rec)
        detailed = pd.DataFrame(rows)
        return detailed[columns + [f"wt_{c}_seconds" for c in CAUSES]]

    @staticmethod
    def _natural_key(value: str) -> list[Any]:
        return [int(x) if x.isdigit() else x for x in re.split(r"(\d+)", value)]


class CTEMeasurer:
    def calculate(self, events: pd.DataFrame, instances: pd.DataFrame) -> dict[str, Any]:
        processing = float(events["processing_seconds"].clip(lower=0).sum())
        waiting = float(instances["wt_total_seconds"].sum()) if not instances.empty else 0.0
        denominator = processing + waiting
        original = safe_div(processing, denominator)
        result: dict[str, Any] = {
            "processing_seconds": processing,
            "waiting_seconds": waiting,
            "cte": original,
            "cte_percent": original * 100,
            "by_cause": {},
        }
        for cause in CAUSES:
            removed = float(instances[f"wt_{cause}_seconds"].sum()) if not instances.empty else 0.0
            without = safe_div(processing, processing + max(0.0, waiting - removed))
            result["by_cause"][cause] = {
                "waiting_seconds": removed,
                "cte_without_cause": without,
                "cte_without_cause_percent": without * 100,
                "cte_impact": without - original,
                "cte_impact_percentage_points": (without - original) * 100,
            }
        return result


class EvaluationRunner:
    KEYS = TransitionAggregator.KEYS

    def __init__(self, tolerance: float):
        self.tolerance = tolerance

    def compare_expected(self, predicted: pd.DataFrame, expected_path: Path) -> tuple[dict[str, Any], pd.DataFrame]:
        expected = pd.read_csv(expected_path, encoding="utf-8-sig")
        for df in (predicted, expected):
            for key in self.KEYS:
                df[key] = df[key].astype(str)
        duration_cols = ["wt_total", "wt_batching", "wt_prioritization", "wt_contention",
                         "wt_unavailability", "wt_extraneous"]
        for col in duration_cols:
            expected[f"{col}_seconds_eval"] = pd.to_timedelta(expected[col]).dt.total_seconds()
            if f"{col}_seconds_eval" not in predicted:
                source = "wt_total_seconds" if col == "wt_total" else f"{col}_seconds"
                predicted[f"{col}_seconds_eval"] = predicted[source].astype(float)
        joined = expected.merge(predicted, on=self.KEYS, how="outer", suffixes=("_expected", "_predicted"), indicator=True)
        rows: list[dict[str, Any]] = []
        cause_items_expected: set[tuple[Any, ...]] = set()
        cause_items_predicted: set[tuple[Any, ...]] = set()
        for _, row in joined.iterrows():
            detail: dict[str, Any] = {k: row.get(k) for k in self.KEYS}
            detail["row_status"] = row["_merge"]
            max_error = 0.0
            all_duration_match = row["_merge"] == "both"
            for col in duration_cols:
                e = row.get(f"{col}_seconds_eval_expected", np.nan)
                p = row.get(f"{col}_seconds_eval_predicted", np.nan)
                error = abs(float(p) - float(e)) if pd.notna(e) and pd.notna(p) else math.inf
                detail[f"{col}_absolute_error_seconds"] = error
                max_error = max(max_error, error)
                all_duration_match &= error <= self.tolerance
                if col != "wt_total":
                    item = tuple(row.get(k) for k in self.KEYS) + (col.replace("wt_", ""),)
                    if pd.notna(e) and float(e) > self.tolerance:
                        cause_items_expected.add(item)
                    if pd.notna(p) and float(p) > self.tolerance:
                        cause_items_predicted.add(item)
            ef = row.get("frequency_expected", np.nan)
            pf = row.get("frequency_predicted", np.nan)
            frequency_match = pd.notna(ef) and pd.notna(pf) and abs(float(ef) - float(pf)) <= 1e-9
            ec = self._case_set(row.get("cases_expected", ""))
            pc = self._case_set(row.get("cases_predicted", ""))
            detail.update({"frequency_match": frequency_match, "case_set_match": ec == pc,
                           "duration_match": bool(all_duration_match), "max_duration_error_seconds": max_error,
                           "full_row_match": bool(all_duration_match and frequency_match and ec == pc)})
            rows.append(detail)
        tp = len(cause_items_expected & cause_items_predicted)
        fp = len(cause_items_predicted - cause_items_expected)
        fn = len(cause_items_expected - cause_items_predicted)
        precision, recall = safe_div(tp, tp + fp), safe_div(tp, tp + fn)
        details = pd.DataFrame(rows)
        metrics = {
            "expected_rows": int(len(expected)), "predicted_rows": int(len(predicted)),
            "matched_key_rows": int((joined["_merge"] == "both").sum()),
            "missing_rows": int((joined["_merge"] == "left_only").sum()),
            "unexpected_rows": int((joined["_merge"] == "right_only").sum()),
            "full_row_matches": int(details["full_row_match"].sum()),
            "full_row_match_rate": safe_div(float(details["full_row_match"].sum()), len(expected)),
            "mean_max_duration_error_seconds": float(details.replace(math.inf, np.nan)["max_duration_error_seconds"].mean()),
            "cause_presence": {"tp": tp, "fp": fp, "fn": fn, "precision": precision,
                               "recall": recall, "f1": safe_div(2 * precision * recall, precision + recall)},
        }
        return metrics, details

    @staticmethod
    def _case_set(value: Any) -> set[str]:
        if pd.isna(value) or str(value).strip() == "":
            return set()
        return {x.strip() for x in str(value).split(",") if x.strip()}

    def evaluate_batches(self, original: pd.DataFrame, instances: pd.DataFrame) -> dict[str, Any]:
        if "batch_instance_id" not in original.columns:
            return {"available": False, "reason": "No batch_instance_id column"}
        truth = set(original.loc[original["batch_instance_id"].notna(), "event_id"].astype(str))
        predicted = set(instances.loc[instances["predicted_batch_id"].astype(str) != "", "target_event_id"].astype(str))
        tp, fp, fn = len(truth & predicted), len(predicted - truth), len(truth - predicted)
        precision, recall = safe_div(tp, tp + fp), safe_div(tp, tp + fn)
        return {"available": True, "expected_instances": len(truth), "predicted_instances": len(predicted),
                "tp": tp, "fp": fp, "fn": fn, "precision": precision, "recall": recall,
                "f1": safe_div(2 * precision * recall, precision + recall)}


# -----------------------------------------------------------------------------
# Pipeline and reporting
# -----------------------------------------------------------------------------
class WaitingTimePipeline:
    def __init__(self, config: AnalysisConfig):
        self.cfg = config
        self.cfg.validate()

    def run(self, input_path: Path, output_dir: Path, expected_path: Optional[Path] = None) -> dict[str, Any]:
        start = time.perf_counter()
        output_dir.mkdir(parents=True, exist_ok=True)
        loader = EventLogLoader(self.cfg)
        events, quality = loader.load(input_path)
        # Preserve ground-truth-only metadata in a copy; analysis never reads it.
        original_with_metadata = events.copy()
        analysis_events = events.drop(columns=["batch_instance_id"], errors="ignore")
        transitions = TransitionDiscoverer(self.cfg).discover(analysis_events)
        instances, predicted_batches = WaitingTimeAnalyzer(self.cfg).analyze(analysis_events, transitions)
        summary = TransitionAggregator().aggregate(instances)
        cte = CTEMeasurer().calculate(analysis_events, instances)

        summary.to_csv(output_dir / "transition_summary.csv", index=False)
        if self.cfg.write_instances:
            instances.to_csv(output_dir / "transition_instances.csv", index=False)
        with (output_dir / "cte_summary.json").open("w", encoding="utf-8") as fh:
            json.dump(cte, fh, indent=2)
        with (output_dir / "data_quality_report.json").open("w", encoding="utf-8") as fh:
            json.dump(quality, fh, indent=2, default=str)

        evaluator = EvaluationRunner(self.cfg.duration_tolerance_seconds)
        evaluation: dict[str, Any] = {"expected_comparison": None,
                                      "batch_ground_truth": evaluator.evaluate_batches(original_with_metadata, instances)}
        if expected_path:
            metrics, details = evaluator.compare_expected(summary.copy(), expected_path)
            evaluation["expected_comparison"] = metrics
            details.to_csv(output_dir / "expected_comparison_details.csv", index=False)
        with (output_dir / "evaluation_metrics.json").open("w", encoding="utf-8") as fh:
            json.dump(evaluation, fh, indent=2, default=str)

        metadata = {
            "program_version": VERSION,
            "executed_at_utc": iso_now(),
            "python": sys.version,
            "platform": platform.platform(),
            "pandas": pd.__version__,
            "numpy": np.__version__,
            "input": str(input_path),
            "input_sha256": file_sha256(input_path),
            "expected": str(expected_path) if expected_path else None,
            "config": asdict(self.cfg),
            "quality": quality,
            "transitions": int(len(instances)),
            "transition_groups": int(len(summary)),
            "predicted_batch_groups": int(len(predicted_batches)),
            "runtime_seconds": time.perf_counter() - start,
        }
        with (output_dir / "run_metadata.json").open("w", encoding="utf-8") as fh:
            json.dump(metadata, fh, indent=2, default=str)
        return {"metadata": metadata, "cte": cte, "evaluation": evaluation,
                "summary_path": str(output_dir / "transition_summary.csv")}

def write_default_config(path: Path) -> None:
    cfg = asdict(AnalysisConfig())
    if yaml is None:
        path.write_text(json.dumps(cfg, indent=2), encoding="utf-8")
    else:
        path.write_text(yaml.safe_dump(cfg, sort_keys=False), encoding="utf-8")

def configure_logging(verbose: bool, output_dir: Optional[Path] = None) -> None:
    handlers: list[logging.Handler] = [logging.StreamHandler()]
    if output_dir:
        output_dir.mkdir(parents=True, exist_ok=True)
        handlers.append(logging.FileHandler(output_dir / "run.log", encoding="utf-8"))
    logging.basicConfig(level=logging.DEBUG if verbose else logging.INFO,
                        format="%(asctime)s %(levelname)s %(name)s: %(message)s", handlers=handlers)

def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Analyze and evaluate waiting-time causes in event logs")
    parser.add_argument("--version", action="version", version=VERSION)
    sub = parser.add_subparsers(dest="command", required=True)

    def common(p: argparse.ArgumentParser) -> None:
        p.add_argument("--input", required=True, type=Path, help="Input activity-instance CSV")
        p.add_argument("--output-dir", required=True, type=Path, help="Directory for generated reports")
        p.add_argument("--config", type=str, help="Optional YAML configuration")
        p.add_argument("--verbose", action="store_true")
        p.add_argument("--transition-method", choices=["adjacent", "closest_non_concurrent"])
        p.add_argument("--calendar-method", choices=["schedule", "inferred", "always"])

    analyze = sub.add_parser("analyze", help="Analyze one event log")
    common(analyze)

    evaluate = sub.add_parser("evaluate", help="Analyze and compare with an expected report")
    common(evaluate)
    evaluate.add_argument("--expected", required=True, type=Path)

    validate = sub.add_parser("validate", help="Validate an event log without analysis")
    validate.add_argument("--input", required=True, type=Path)
    validate.add_argument("--config", type=str)
    validate.add_argument("--verbose", action="store_true")

    init = sub.add_parser("init-config", help="Write a default YAML/JSON configuration")
    init.add_argument("--output", required=True, type=Path)
    return parser

def apply_cli_overrides(cfg: AnalysisConfig, args: argparse.Namespace) -> AnalysisConfig:
    if getattr(args, "transition_method", None):
        cfg.transition_method = args.transition_method
    if getattr(args, "calendar_method", None):
        cfg.calendar_method = args.calendar_method
    return cfg

def main(argv: Optional[Sequence[str]] = None) -> int:
    args = build_parser().parse_args(argv)
    if args.command == "init-config":
        args.output.parent.mkdir(parents=True, exist_ok=True)
        write_default_config(args.output)
        print(args.output)
        return 0

    cfg = apply_cli_overrides(AnalysisConfig.from_yaml(getattr(args, "config", None)), args)
    configure_logging(getattr(args, "verbose", False), getattr(args, "output_dir", None))
    try:
        if args.command == "validate":
            events, quality = EventLogLoader(cfg).load(args.input)
            print(json.dumps(quality, indent=2, default=str))
            return 0
        result = WaitingTimePipeline(cfg).run(args.input, args.output_dir,
                                              getattr(args, "expected", None))
        print(json.dumps(result, indent=2, default=str))
        return 0
    except Exception as exc:
        LOG.exception("Analysis failed: %s", exc)
        return 1

if __name__ == "__main__":
    raise SystemExit(main())