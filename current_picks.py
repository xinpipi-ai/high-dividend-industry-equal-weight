from __future__ import annotations

import argparse
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import pandas as pd

import backtest_tushare as strategy


ROOT = Path(__file__).resolve().parent
OUTPUT_DIR = ROOT / "outputs"
CN_TZ = ZoneInfo("Asia/Shanghai")


DISPLAY_COLUMNS = [
    "ts_code",
    "name",
    "l1_name",
    "dv_ttm",
    "roe",
    "netprofit_yoy",
    "ocfps",
    "total_mv",
]


def today_yyyymmdd() -> str:
    return datetime.now(CN_TZ).strftime(strategy.DATE_FMT)


def format_percent(value: float) -> str:
    return f"{value:.2f}%"


def format_number(value: float) -> str:
    return f"{value:.2f}"


def to_markdown_table(df: pd.DataFrame) -> str:
    rows = []
    header = ["股票代码", "名称", "行业", "股息率代理", "ROE", "净利润同比", "每股经营现金流", "总市值"]
    rows.append("| " + " | ".join(header) + " |")
    rows.append("|" + "|".join(["---"] * len(header)) + "|")
    for _, row in df.iterrows():
        rows.append(
            "| "
            + " | ".join(
                [
                    str(row["ts_code"]),
                    str(row["name"]),
                    str(row["l1_name"]),
                    format_percent(float(row["dv_ttm"])),
                    format_percent(float(row["roe"])),
                    format_percent(float(row["netprofit_yoy"])),
                    format_number(float(row["ocfps"])),
                    format_number(float(row["total_mv"])),
                ]
            )
            + " |"
        )
    return "\n".join(rows)


def pick_current(args: argparse.Namespace) -> tuple[str, pd.DataFrame]:
    end_date = args.end_date or today_yyyymmdd()
    cfg = strategy.Config(
        start_date=args.start_date,
        end_date=end_date,
        target_count=args.target_count,
        stocks_per_industry=args.stocks_per_industry,
        benchmark=args.benchmark,
        sleep=args.sleep,
    )

    pro = strategy.pro_api()
    dates = strategy.trade_dates(pro, cfg)
    if not dates:
        raise RuntimeError(f"No trading dates found before {end_date}.")
    latest_trade_date = dates[-1]

    universe = strategy.stock_universe(pro, cfg.sleep)
    members = strategy.industry_members(pro, cfg.sleep)
    picks = strategy.pick_stocks(pro, cfg, latest_trade_date, universe, members)
    picks = picks[DISPLAY_COLUMNS].copy()
    return latest_trade_date, picks


def write_outputs(trade_date: str, picks: pd.DataFrame, output_dir: Path) -> tuple[Path, Path]:
    output_dir.mkdir(parents=True, exist_ok=True)
    csv_path = output_dir / "current_picks.csv"
    md_path = output_dir / "current_picks.md"

    picks.to_csv(csv_path, index=False)
    md = [
        f"# Current Strategy Picks",
        "",
        f"Latest trade date: `{trade_date}`",
        "",
        "These are rule-based strategy picks, not investment advice.",
        "",
        to_markdown_table(picks),
        "",
    ]
    md_path.write_text("\n".join(md), encoding="utf-8")
    return csv_path, md_path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate the latest monthly stock picks for the strategy.")
    parser.add_argument("--start-date", default="20190101", help="Historical start date for financial data lookback.")
    parser.add_argument("--end-date", help="Pick the latest open trading day on or before this YYYYMMDD date.")
    parser.add_argument("--target-count", type=int, default=10)
    parser.add_argument("--stocks-per-industry", type=int, default=1)
    parser.add_argument("--benchmark", default="000300.SH")
    parser.add_argument("--sleep", type=float, default=0.12)
    parser.add_argument("--output-dir", default=str(OUTPUT_DIR))
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    trade_date, picks = pick_current(args)
    csv_path, md_path = write_outputs(trade_date, picks, Path(args.output_dir))
    print(f"Latest trade date: {trade_date}")
    print(picks.to_string(index=False, float_format=lambda value: f"{value:.2f}"))
    print(f"Wrote: {csv_path}")
    print(f"Wrote: {md_path}")


if __name__ == "__main__":
    main()
