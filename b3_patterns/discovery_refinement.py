from __future__ import annotations

import csv
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path

from .discovery import _iter_discovery_samples, build_option_discovery_templates
from .models import (
    DiscoveryPatternSummary,
    DiscoveryRefinedSummary,
    DiscoveryTemplateBaseline,
    StrategyTrade,
)


@dataclass(slots=True)
class _TradeAggregate:
    count: int = 0
    net: float = 0.0
    gross_profit: float = 0.0
    gross_loss: float = 0.0


@dataclass(slots=True)
class _RefinementAccumulator:
    train: _TradeAggregate
    validation: _TradeAggregate
    monthly_net: dict[str, float]
    validation_monthly_net: dict[str, float]


def _parse_profit_factor(raw_value: str) -> float | None:
    cleaned = (raw_value or "").strip()
    if not cleaned:
        return None
    if cleaned == "INF":
        return float("inf")
    return float(cleaned)


def _calculate_profit_factor(gross_profit: float, gross_loss: float) -> float | None:
    if gross_loss == 0:
        if gross_profit == 0:
            return None
        return float("inf")
    return gross_profit / gross_loss


def load_discovery_summaries(path: str | Path) -> list[DiscoveryPatternSummary]:
    csv_path = Path(path)
    if not csv_path.exists():
        return []

    results: list[DiscoveryPatternSummary] = []
    with csv_path.open("r", encoding="utf-8", newline="") as file_obj:
        reader = csv.DictReader(file_obj)
        for row in reader:
            results.append(
                DiscoveryPatternSummary(
                    code=row["code"],
                    label=row["label"],
                    family=row["family"],
                    template_code=row["template_code"],
                    template_label=row["template_label"],
                    option_side=row["option_side"],
                    dte_target_days=int(row["dte_target_days"]),
                    trade_direction=row["trade_direction"],
                    entry_rule=row["entry_rule"],
                    exit_offset_days=int(row["exit_offset_days"]),
                    state_size=int(row["state_size"]),
                    state_signature=row["state_signature"],
                    feature_keys=row["feature_keys"],
                    min_trade_return_pct=float(row["min_trade_return_pct"]),
                    tickers_with_matches=int(row["tickers_with_matches"]),
                    total_occurrences=int(row["total_occurrences"]),
                    successful_occurrences=int(row["successful_occurrences"]),
                    success_rate_pct=float(row["success_rate_pct"]),
                    profitable_trades=int(row["profitable_trades"]),
                    profitable_trade_rate_pct=float(row["profitable_trade_rate_pct"]),
                    average_asset_move_pct=float(row["average_asset_move_pct"]),
                    average_trade_return_pct=float(row["average_trade_return_pct"]),
                    net_trade_return_pct=float(row["net_trade_return_pct"]),
                    cumulative_return_pct=float(row["cumulative_return_pct"]),
                    profit_factor=_parse_profit_factor(row.get("profit_factor", "")),
                    first_trade_date=row["first_trade_date"],
                    last_trade_date=row["last_trade_date"],
                )
            )
    return results


def summarize_template_baselines(
    *,
    db_path: str | Path,
    tickers_file: str | Path,
    start_date: str,
    end_date: str,
    dte_targets: list[int],
    round_trip_cost_pct: float = 0.0,
) -> dict[str, DiscoveryTemplateBaseline]:
    templates = build_option_discovery_templates(
        round_trip_cost_pct=round_trip_cost_pct,
        dte_targets=dte_targets,
    )
    grouped: dict[str, dict[str, object]] = {
        template.code: {
            "template": template,
            "count": 0,
            "profit": 0.0,
            "loss": 0.0,
            "net": 0.0,
            "profitable": 0,
            "first_trade_date": None,
            "last_trade_date": None,
        }
        for template in templates
    }

    for sample in _iter_discovery_samples(
        db_path=db_path,
        tickers_file=tickers_file,
        start_date=start_date,
        end_date=end_date,
        template_definitions=templates,
    ):
        item = grouped[sample["template"].code]
        item["count"] += 1
        trade_return = sample["trade_return_pct"]
        item["net"] += trade_return
        if trade_return > 0:
            item["profitable"] += 1
            item["profit"] += trade_return
        elif trade_return < 0:
            item["loss"] += -trade_return
        trade_date = sample["trade_date"]
        if item["first_trade_date"] is None or trade_date < item["first_trade_date"]:
            item["first_trade_date"] = trade_date
        if item["last_trade_date"] is None or trade_date > item["last_trade_date"]:
            item["last_trade_date"] = trade_date

    baselines: dict[str, DiscoveryTemplateBaseline] = {}
    for template_code, item in grouped.items():
        total_occurrences = int(item["count"])
        if total_occurrences == 0:
            continue
        template = item["template"]
        profitable_trades = int(item["profitable"])
        profit_factor = _calculate_profit_factor(
            gross_profit=float(item["profit"]),
            gross_loss=float(item["loss"]),
        )
        baselines[template_code] = DiscoveryTemplateBaseline(
            template_code=template.code,
            template_label=template.label,
            option_side=template.option_side,
            dte_target_days=template.dte_target_days,
            total_occurrences=total_occurrences,
            profitable_trades=profitable_trades,
            profitable_trade_rate_pct=(profitable_trades / total_occurrences) * 100.0,
            average_trade_return_pct=float(item["net"]) / total_occurrences,
            net_trade_return_pct=float(item["net"]),
            profit_factor=profit_factor,
            first_trade_date=str(item["first_trade_date"] or ""),
            last_trade_date=str(item["last_trade_date"] or ""),
        )
    return baselines


def _prefilter_candidates(
    summaries: list[DiscoveryPatternSummary],
    baselines: dict[str, DiscoveryTemplateBaseline],
    *,
    min_state_size: int,
    min_avg_uplift_pct: float,
    min_pf_uplift: float,
    min_total_trades: int,
    min_tickers: int,
) -> tuple[list[DiscoveryPatternSummary], dict[str, list[str]]]:
    candidates: list[DiscoveryPatternSummary] = []
    rejection_reasons: dict[str, list[str]] = {}

    for summary in summaries:
        reasons: list[str] = []
        baseline = baselines.get(summary.template_code)
        if baseline is None:
            reasons.append("missing_baseline")
        else:
            avg_uplift = summary.average_trade_return_pct - baseline.average_trade_return_pct
            baseline_pf = baseline.profit_factor or 0.0
            summary_pf = summary.profit_factor or 0.0
            pf_uplift = summary_pf - baseline_pf
            if summary.state_size < min_state_size:
                reasons.append(f"state_size<{min_state_size}")
            if avg_uplift < min_avg_uplift_pct:
                reasons.append(f"avg_uplift<{min_avg_uplift_pct:.2f}")
            if pf_uplift < min_pf_uplift:
                reasons.append(f"pf_uplift<{min_pf_uplift:.2f}")
        if summary.total_occurrences < min_total_trades:
            reasons.append(f"trades<{min_total_trades}")
        if summary.tickers_with_matches < min_tickers:
            reasons.append(f"tickers<{min_tickers}")

        if reasons:
            rejection_reasons[summary.code] = reasons
            continue
        candidates.append(summary)

    return candidates, rejection_reasons


def _stream_trade_aggregates(
    *,
    trades_csv_path: str | Path,
    candidate_codes: set[str],
    validation_start_date: str,
) -> dict[str, _RefinementAccumulator]:
    aggregates: dict[str, _RefinementAccumulator] = {
        code: _RefinementAccumulator(
            train=_TradeAggregate(),
            validation=_TradeAggregate(),
            monthly_net=defaultdict(float),
            validation_monthly_net=defaultdict(float),
        )
        for code in candidate_codes
    }
    csv_path = Path(trades_csv_path)
    with csv_path.open("r", encoding="utf-8", newline="") as file_obj:
        reader = csv.DictReader(file_obj)
        for row in reader:
            strategy_code = row["strategy_code"]
            accumulator = aggregates.get(strategy_code)
            if accumulator is None:
                continue
            trade_return = float(row["trade_return_pct"])
            trigger_date = row["trigger_date"]
            trade_month = trigger_date[:7]

            bucket = accumulator.train if trigger_date < validation_start_date else accumulator.validation
            bucket.count += 1
            bucket.net += trade_return
            if trade_return > 0:
                bucket.gross_profit += trade_return
            elif trade_return < 0:
                bucket.gross_loss += -trade_return

            accumulator.monthly_net[trade_month] += trade_return
            if trigger_date >= validation_start_date:
                accumulator.validation_monthly_net[trade_month] += trade_return

    return aggregates


def _build_refined_summaries(
    *,
    candidates: list[DiscoveryPatternSummary],
    baselines: dict[str, DiscoveryTemplateBaseline],
    aggregates: dict[str, _RefinementAccumulator],
    min_train_trades: int,
    min_validation_trades: int,
    min_train_profit_factor: float,
    min_validation_profit_factor: float,
    min_train_average_trade_return_pct: float,
    min_validation_average_trade_return_pct: float,
    min_active_months: int,
    min_positive_month_ratio: float,
    min_validation_active_months: int,
    min_validation_positive_month_ratio: float,
) -> tuple[list[DiscoveryRefinedSummary], dict[str, list[str]]]:
    refined: list[DiscoveryRefinedSummary] = []
    rejection_reasons: dict[str, list[str]] = {}

    for summary in candidates:
        baseline = baselines[summary.template_code]
        accumulator = aggregates.get(summary.code)
        reasons: list[str] = []
        if accumulator is None:
            reasons.append("missing_trades")
            rejection_reasons[summary.code] = reasons
            continue

        train_pf = _calculate_profit_factor(
            gross_profit=accumulator.train.gross_profit,
            gross_loss=accumulator.train.gross_loss,
        )
        validation_pf = _calculate_profit_factor(
            gross_profit=accumulator.validation.gross_profit,
            gross_loss=accumulator.validation.gross_loss,
        )
        train_avg = (
            accumulator.train.net / accumulator.train.count
            if accumulator.train.count
            else 0.0
        )
        validation_avg = (
            accumulator.validation.net / accumulator.validation.count
            if accumulator.validation.count
            else 0.0
        )
        positive_months = sum(1 for value in accumulator.monthly_net.values() if value > 0)
        active_months = len(accumulator.monthly_net)
        positive_month_ratio = (
            positive_months / active_months if active_months else 0.0
        )
        validation_positive_months = sum(
            1 for value in accumulator.validation_monthly_net.values() if value > 0
        )
        validation_active_months = len(accumulator.validation_monthly_net)
        validation_positive_month_ratio = (
            validation_positive_months / validation_active_months
            if validation_active_months
            else 0.0
        )

        if accumulator.train.count < min_train_trades:
            reasons.append(f"train_trades<{min_train_trades}")
        if accumulator.validation.count < min_validation_trades:
            reasons.append(f"validation_trades<{min_validation_trades}")
        if train_pf is None or train_pf < min_train_profit_factor:
            reasons.append(f"train_pf<{min_train_profit_factor:.2f}")
        if validation_pf is None or validation_pf < min_validation_profit_factor:
            reasons.append(f"validation_pf<{min_validation_profit_factor:.2f}")
        if train_avg < min_train_average_trade_return_pct:
            reasons.append(f"train_avg<{min_train_average_trade_return_pct:.2f}")
        if validation_avg < min_validation_average_trade_return_pct:
            reasons.append(f"validation_avg<{min_validation_average_trade_return_pct:.2f}")
        if accumulator.train.net <= 0:
            reasons.append("train_net<=0")
        if accumulator.validation.net <= 0:
            reasons.append("validation_net<=0")
        if active_months < min_active_months:
            reasons.append(f"active_months<{min_active_months}")
        if positive_month_ratio < min_positive_month_ratio:
            reasons.append(f"positive_month_ratio<{min_positive_month_ratio:.2f}")
        if validation_active_months < min_validation_active_months:
            reasons.append(f"validation_active_months<{min_validation_active_months}")
        if validation_positive_month_ratio < min_validation_positive_month_ratio:
            reasons.append(
                f"validation_positive_month_ratio<{min_validation_positive_month_ratio:.2f}"
            )

        if reasons:
            rejection_reasons[summary.code] = reasons
            continue

        baseline_pf = baseline.profit_factor or 0.0
        summary_pf = summary.profit_factor or 0.0
        avg_uplift = summary.average_trade_return_pct - baseline.average_trade_return_pct
        pf_uplift = summary_pf - baseline_pf
        robustness_score = (
            (avg_uplift * 5.0)
            + (pf_uplift * 20.0)
            + (validation_avg * 3.0)
            + (validation_positive_month_ratio * 10.0)
            + (summary.net_trade_return_pct / max(summary.total_occurrences, 1))
        )

        refined.append(
            DiscoveryRefinedSummary(
                code=summary.code,
                label=summary.label,
                family=summary.family,
                template_code=summary.template_code,
                template_label=summary.template_label,
                option_side=summary.option_side,
                dte_target_days=summary.dte_target_days,
                trade_direction=summary.trade_direction,
                state_size=summary.state_size,
                state_signature=summary.state_signature,
                feature_keys=summary.feature_keys,
                tickers_with_matches=summary.tickers_with_matches,
                total_occurrences=summary.total_occurrences,
                profitable_trade_rate_pct=summary.profitable_trade_rate_pct,
                average_trade_return_pct=summary.average_trade_return_pct,
                net_trade_return_pct=summary.net_trade_return_pct,
                profit_factor=summary.profit_factor,
                baseline_average_trade_return_pct=baseline.average_trade_return_pct,
                baseline_profit_factor=baseline.profit_factor,
                average_trade_uplift_pct=avg_uplift,
                profit_factor_uplift=pf_uplift,
                train_trades=accumulator.train.count,
                train_average_trade_return_pct=train_avg,
                train_net_trade_return_pct=accumulator.train.net,
                train_profit_factor=train_pf,
                validation_trades=accumulator.validation.count,
                validation_average_trade_return_pct=validation_avg,
                validation_net_trade_return_pct=accumulator.validation.net,
                validation_profit_factor=validation_pf,
                active_months=active_months,
                positive_months=positive_months,
                positive_month_ratio=positive_month_ratio,
                validation_active_months=validation_active_months,
                validation_positive_months=validation_positive_months,
                validation_positive_month_ratio=validation_positive_month_ratio,
                overlap_bucket="pending",
                robustness_score=robustness_score,
            )
        )

    return refined, rejection_reasons


def _collect_trade_key_sets(
    *,
    trades_csv_path: str | Path,
    candidate_codes: set[str],
) -> dict[str, set[tuple[str, str, str]]]:
    key_sets: dict[str, set[tuple[str, str, str]]] = {
        code: set() for code in candidate_codes
    }
    csv_path = Path(trades_csv_path)
    with csv_path.open("r", encoding="utf-8", newline="") as file_obj:
        reader = csv.DictReader(file_obj)
        for row in reader:
            strategy_code = row["strategy_code"]
            key_set = key_sets.get(strategy_code)
            if key_set is None:
                continue
            key_set.add((row["ticker"], row["trigger_date"], row["instrument_symbol"]))
    return key_sets


def deduplicate_refined_patterns(
    refined_summaries: list[DiscoveryRefinedSummary],
    trade_key_sets: dict[str, set[tuple[str, str, str]]],
    *,
    overlap_threshold: float = 0.85,
) -> tuple[list[DiscoveryRefinedSummary], dict[str, list[str]]]:
    selected: list[DiscoveryRefinedSummary] = []
    rejection_reasons: dict[str, list[str]] = {}

    ranked = sorted(
        refined_summaries,
        key=lambda item: (
            item.robustness_score,
            item.validation_net_trade_return_pct,
            item.average_trade_uplift_pct,
            item.profit_factor_uplift if item.profit_factor_uplift is not None else float("-inf"),
        ),
        reverse=True,
    )

    for candidate in ranked:
        candidate_keys = trade_key_sets.get(candidate.code, set())
        skip_candidate = False
        for selected_item in selected:
            if selected_item.template_code != candidate.template_code:
                continue
            selected_keys = trade_key_sets.get(selected_item.code, set())
            if not candidate_keys or not selected_keys:
                continue
            overlap = len(candidate_keys & selected_keys) / min(
                len(candidate_keys),
                len(selected_keys),
            )
            if overlap >= overlap_threshold:
                rejection_reasons[candidate.code] = [
                    f"overlap>={overlap_threshold:.2f}",
                    f"duplicate_of={selected_item.code}",
                ]
                skip_candidate = True
                break
        if skip_candidate:
            continue
        selected.append(candidate)

    deduped: list[DiscoveryRefinedSummary] = []
    for item in selected:
        overlap_bucket = (
            "specific"
            if item.average_trade_uplift_pct >= 1.0 and (item.profit_factor_uplift or 0.0) >= 0.20
            else "broad"
        )
        deduped.append(
            DiscoveryRefinedSummary(
                code=item.code,
                label=item.label,
                family=item.family,
                template_code=item.template_code,
                template_label=item.template_label,
                option_side=item.option_side,
                dte_target_days=item.dte_target_days,
                trade_direction=item.trade_direction,
                state_size=item.state_size,
                state_signature=item.state_signature,
                feature_keys=item.feature_keys,
                tickers_with_matches=item.tickers_with_matches,
                total_occurrences=item.total_occurrences,
                profitable_trade_rate_pct=item.profitable_trade_rate_pct,
                average_trade_return_pct=item.average_trade_return_pct,
                net_trade_return_pct=item.net_trade_return_pct,
                profit_factor=item.profit_factor,
                baseline_average_trade_return_pct=item.baseline_average_trade_return_pct,
                baseline_profit_factor=item.baseline_profit_factor,
                average_trade_uplift_pct=item.average_trade_uplift_pct,
                profit_factor_uplift=item.profit_factor_uplift,
                train_trades=item.train_trades,
                train_average_trade_return_pct=item.train_average_trade_return_pct,
                train_net_trade_return_pct=item.train_net_trade_return_pct,
                train_profit_factor=item.train_profit_factor,
                validation_trades=item.validation_trades,
                validation_average_trade_return_pct=item.validation_average_trade_return_pct,
                validation_net_trade_return_pct=item.validation_net_trade_return_pct,
                validation_profit_factor=item.validation_profit_factor,
                active_months=item.active_months,
                positive_months=item.positive_months,
                positive_month_ratio=item.positive_month_ratio,
                validation_active_months=item.validation_active_months,
                validation_positive_months=item.validation_positive_months,
                validation_positive_month_ratio=item.validation_positive_month_ratio,
                overlap_bucket=overlap_bucket,
                robustness_score=item.robustness_score,
            )
        )

    return deduped, rejection_reasons


def collect_refined_trades(
    *,
    trades_csv_path: str | Path,
    selected_codes: set[str],
) -> list[StrategyTrade]:
    if not selected_codes:
        return []

    trades: list[StrategyTrade] = []
    csv_path = Path(trades_csv_path)
    with csv_path.open("r", encoding="utf-8", newline="") as file_obj:
        reader = csv.DictReader(file_obj)
        for row in reader:
            if row["strategy_code"] not in selected_codes:
                continue
            trades.append(
                StrategyTrade(
                    strategy_code=row["strategy_code"],
                    strategy_label=row["strategy_label"],
                    family=row["family"],
                    ticker=row["ticker"],
                    trigger_date=row["trigger_date"],
                    exit_date=row["exit_date"],
                    direction=row["direction"],
                    trigger_change_pct=float(row["trigger_change_pct"]),
                    entry_price=float(row["entry_price"]),
                    exit_price=float(row["exit_price"]),
                    asset_move_pct=float(row["asset_move_pct"]),
                    trade_return_pct=float(row["trade_return_pct"]),
                    is_profitable=str(row["is_profitable"]).lower() == "true",
                    is_successful=str(row["is_successful"]).lower() == "true",
                    instrument_symbol=row.get("instrument_symbol") or None,
                    contract_expiration=row.get("contract_expiration") or None,
                    dte_target_days=(
                        None if not row.get("dte_target_days") else int(row["dte_target_days"])
                    ),
                    exit_reason=row.get("exit_reason") or None,
                )
            )
    return trades


def refine_discovery_shortlist(
    *,
    summaries: list[DiscoveryPatternSummary],
    trades_csv_path: str | Path,
    baselines: dict[str, DiscoveryTemplateBaseline],
    validation_start_date: str,
    min_state_size: int = 2,
    min_avg_uplift_pct: float = 1.0,
    min_pf_uplift: float = 0.20,
    min_total_trades: int = 300,
    min_tickers: int = 30,
    min_train_trades: int = 150,
    min_validation_trades: int = 40,
    min_train_profit_factor: float = 1.10,
    min_validation_profit_factor: float = 1.10,
    min_train_average_trade_return_pct: float = 0.0,
    min_validation_average_trade_return_pct: float = 0.0,
    min_active_months: int = 6,
    min_positive_month_ratio: float = 0.55,
    min_validation_active_months: int = 2,
    min_validation_positive_month_ratio: float = 0.50,
    overlap_threshold: float = 0.85,
) -> tuple[list[DiscoveryRefinedSummary], dict[str, list[str]], list[StrategyTrade]]:
    candidates, prefilter_rejections = _prefilter_candidates(
        summaries=summaries,
        baselines=baselines,
        min_state_size=min_state_size,
        min_avg_uplift_pct=min_avg_uplift_pct,
        min_pf_uplift=min_pf_uplift,
        min_total_trades=min_total_trades,
        min_tickers=min_tickers,
    )
    aggregates = _stream_trade_aggregates(
        trades_csv_path=trades_csv_path,
        candidate_codes={item.code for item in candidates},
        validation_start_date=validation_start_date,
    )
    refined, split_rejections = _build_refined_summaries(
        candidates=candidates,
        baselines=baselines,
        aggregates=aggregates,
        min_train_trades=min_train_trades,
        min_validation_trades=min_validation_trades,
        min_train_profit_factor=min_train_profit_factor,
        min_validation_profit_factor=min_validation_profit_factor,
        min_train_average_trade_return_pct=min_train_average_trade_return_pct,
        min_validation_average_trade_return_pct=min_validation_average_trade_return_pct,
        min_active_months=min_active_months,
        min_positive_month_ratio=min_positive_month_ratio,
        min_validation_active_months=min_validation_active_months,
        min_validation_positive_month_ratio=min_validation_positive_month_ratio,
    )
    trade_key_sets = _collect_trade_key_sets(
        trades_csv_path=trades_csv_path,
        candidate_codes={item.code for item in refined},
    )
    deduped, overlap_rejections = deduplicate_refined_patterns(
        refined_summaries=refined,
        trade_key_sets=trade_key_sets,
        overlap_threshold=overlap_threshold,
    )
    selected_codes = {item.code for item in deduped}
    trades = collect_refined_trades(
        trades_csv_path=trades_csv_path,
        selected_codes=selected_codes,
    )
    merged_rejections = dict(prefilter_rejections)
    merged_rejections.update(split_rejections)
    merged_rejections.update(overlap_rejections)
    return deduped, merged_rejections, trades
