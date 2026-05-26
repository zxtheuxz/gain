from __future__ import annotations

import csv
from datetime import datetime
from pathlib import Path

from .models import StrategyRegistryEntry, StrategySummary


def load_registry_entries(path: str | Path) -> list[StrategyRegistryEntry]:
    registry_path = Path(path)
    if not registry_path.exists():
        return []

    entries: list[StrategyRegistryEntry] = []
    with registry_path.open("r", encoding="utf-8", newline="") as file_obj:
        reader = csv.DictReader(file_obj)
        for row in reader:
            profit_factor_raw = row.get("profit_factor", "")
            entries.append(
                StrategyRegistryEntry(
                    code=row["code"],
                    label=row["label"],
                    family=row["family"],
                    setup_kind=row["setup_kind"],
                    trigger_direction=row["trigger_direction"],
                    threshold_pct=float(row["threshold_pct"]),
                    trade_direction=row["trade_direction"],
                    entry_rule=row["entry_rule"],
                    exit_offset_days=int(row["exit_offset_days"]),
                    min_trade_return_pct=float(row["min_trade_return_pct"]),
                    status=row["status"],
                    rejection_reasons=row.get("rejection_reasons", ""),
                    tested_at=row["tested_at"],
                    total_occurrences=int(row["total_occurrences"]),
                    success_rate_pct=float(row["success_rate_pct"]),
                    profitable_trade_rate_pct=float(row["profitable_trade_rate_pct"]),
                    average_trade_return_pct=float(row["average_trade_return_pct"]),
                    net_trade_return_pct=float(row["net_trade_return_pct"]),
                    profit_factor=(
                        None
                        if not profit_factor_raw
                        else float("inf") if profit_factor_raw == "INF"
                        else float(profit_factor_raw)
                    ),
                )
            )

    return entries


def get_known_strategy_codes(entries: list[StrategyRegistryEntry]) -> set[str]:
    return {item.code for item in entries}


def build_registry_entries(
    approved_summaries: list[StrategySummary],
    rejected_summaries: list[StrategySummary],
    rejection_reasons: dict[str, list[str]],
    tested_at: datetime | None = None,
) -> list[StrategyRegistryEntry]:
    evaluated_at = (tested_at or datetime.now().astimezone()).isoformat(timespec="seconds")
    entries: list[StrategyRegistryEntry] = []

    for summary in approved_summaries:
        entries.append(
            StrategyRegistryEntry(
                code=summary.code,
                label=summary.label,
                family=summary.family,
                setup_kind=summary.setup_kind,
                trigger_direction=summary.trigger_direction,
                threshold_pct=summary.threshold_pct,
                trade_direction=summary.trade_direction,
                entry_rule=summary.entry_rule,
                exit_offset_days=summary.exit_offset_days,
                min_trade_return_pct=summary.min_trade_return_pct,
                status="approved",
                rejection_reasons="",
                tested_at=evaluated_at,
                total_occurrences=summary.total_occurrences,
                success_rate_pct=summary.success_rate_pct,
                profitable_trade_rate_pct=summary.profitable_trade_rate_pct,
                average_trade_return_pct=summary.average_trade_return_pct,
                net_trade_return_pct=summary.net_trade_return_pct,
                profit_factor=summary.profit_factor,
            )
        )

    for summary in rejected_summaries:
        entries.append(
            StrategyRegistryEntry(
                code=summary.code,
                label=summary.label,
                family=summary.family,
                setup_kind=summary.setup_kind,
                trigger_direction=summary.trigger_direction,
                threshold_pct=summary.threshold_pct,
                trade_direction=summary.trade_direction,
                entry_rule=summary.entry_rule,
                exit_offset_days=summary.exit_offset_days,
                min_trade_return_pct=summary.min_trade_return_pct,
                status="rejected",
                rejection_reasons=";".join(rejection_reasons.get(summary.code, [])),
                tested_at=evaluated_at,
                total_occurrences=summary.total_occurrences,
                success_rate_pct=summary.success_rate_pct,
                profitable_trade_rate_pct=summary.profitable_trade_rate_pct,
                average_trade_return_pct=summary.average_trade_return_pct,
                net_trade_return_pct=summary.net_trade_return_pct,
                profit_factor=summary.profit_factor,
            )
        )

    return sorted(entries, key=lambda item: (item.status, item.code))


def merge_registry_entries(
    existing_entries: list[StrategyRegistryEntry],
    new_entries: list[StrategyRegistryEntry],
) -> list[StrategyRegistryEntry]:
    merged: dict[str, StrategyRegistryEntry] = {item.code: item for item in existing_entries}
    for item in new_entries:
        merged[item.code] = item
    return sorted(merged.values(), key=lambda item: (item.status, item.code))
