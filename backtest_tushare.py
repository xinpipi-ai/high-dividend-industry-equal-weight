from __future__ import annotations

import argparse
import json
import os
import time
from dataclasses import dataclass
from pathlib import Path

import numpy as np
import pandas as pd

try:
    import tushare as ts
except ImportError as exc:  # pragma: no cover
    raise SystemExit("Missing dependency: pip install tushare pandas numpy") from exc


ROOT = Path(__file__).resolve().parent
CACHE_DIR = ROOT / "data" / "cache"
OUTPUT_DIR = ROOT / "outputs"
DATE_FMT = "%Y%m%d"


@dataclass(frozen=True)
class Config:
    start_date: str
    end_date: str
    target_count: int
    stocks_per_industry: int
    benchmark: str
    sleep: float


def read_token() -> str:
    token = os.getenv("TUSHARE_TOKEN") or os.getenv("TUSHARE_API_KEY")
    if token:
        return token

    env_path = ROOT / ".env"
    if env_path.exists():
        for line in env_path.read_text(encoding="utf-8").splitlines():
            if line.startswith("TUSHARE_TOKEN=") or line.startswith("TUSHARE_API_KEY="):
                return line.split("=", 1)[1].strip()
    raise RuntimeError("Missing Tushare token. Set TUSHARE_TOKEN/TUSHARE_API_KEY or create a local .env file.")


def pro_api():
    return ts.pro_api(read_token())


def cache_path(name: str) -> Path:
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    return CACHE_DIR / f"{name}.csv"


def read_cache(name: str) -> pd.DataFrame | None:
    path = cache_path(name)
    if path.exists():
        return pd.read_csv(path, dtype={"trade_date": str, "cal_date": str, "ann_date": str})
    return None


def write_cache(name: str, df: pd.DataFrame) -> pd.DataFrame:
    path = cache_path(name)
    df.to_csv(path, index=False)
    return df


def call_with_retry(func, sleep: float, **kwargs) -> pd.DataFrame:
    last_error: Exception | None = None
    for _ in range(3):
        try:
            df = func(**kwargs)
            time.sleep(sleep)
            return df if df is not None else pd.DataFrame()
        except Exception as exc:  # noqa: BLE001 - Tushare wraps many API errors as Exception
            last_error = exc
            time.sleep(max(1.0, sleep * 3))
    raise last_error  # type: ignore[misc]


def trade_dates(pro, cfg: Config) -> list[str]:
    key = f"trade_cal_{cfg.start_date}_{cfg.end_date}"
    cached = read_cache(key)
    if cached is None:
        cached = write_cache(
            key,
            call_with_retry(
                pro.trade_cal,
                cfg.sleep,
                exchange="SSE",
                start_date=cfg.start_date,
                end_date=cfg.end_date,
                is_open="1",
            ),
        )
    return sorted(cached["cal_date"].astype(str).tolist())


def month_rebalance_dates(dates: list[str]) -> list[str]:
    frame = pd.DataFrame({"trade_date": dates})
    frame["month"] = frame["trade_date"].str.slice(0, 6)
    return frame.groupby("month")["trade_date"].min().tolist()


def stock_universe(pro, sleep: float) -> pd.DataFrame:
    cached = read_cache("stock_universe")
    if cached is None:
        cached = write_cache(
            "stock_universe",
            call_with_retry(
                pro.stock_basic,
                sleep,
                list_status="L",
                fields="ts_code,name,list_date,market,exchange",
            ),
        )
    out = cached.copy()
    out["list_date"] = out["list_date"].astype(str)
    out = out[~out["name"].fillna("").str.contains("ST|退", regex=True)]
    out = out[~out["ts_code"].str.startswith(("4", "8", "68"))]
    return out


def industry_members(pro, sleep: float) -> pd.DataFrame:
    cached = read_cache("sw2021_l1_members")
    if cached is not None:
        return cached

    industries = call_with_retry(pro.index_classify, sleep, level="L1", src="SW2021")
    frames: list[pd.DataFrame] = []
    for _, row in industries.iterrows():
        members = call_with_retry(pro.index_member_all, sleep, l1_code=row["index_code"], is_new="Y")
        if members.empty:
            continue
        subset = members[["ts_code", "l1_code", "l1_name"]].drop_duplicates()
        frames.append(subset)
    if not frames:
        raise RuntimeError("No SW2021 industry membership returned.")
    return write_cache("sw2021_l1_members", pd.concat(frames, ignore_index=True).drop_duplicates("ts_code"))


def daily_basic_on(pro, trade_date: str, sleep: float) -> pd.DataFrame:
    key = f"daily_basic_{trade_date}"
    cached = read_cache(key)
    if cached is not None:
        return cached
    return write_cache(
        key,
        call_with_retry(
            pro.daily_basic,
            sleep,
            trade_date=trade_date,
            fields="ts_code,trade_date,total_mv,dv_ttm,pe_ttm,pb",
        ),
    )


def close_on(pro, trade_date: str, sleep: float) -> pd.DataFrame:
    key = f"daily_close_{trade_date}"
    cached = read_cache(key)
    if cached is not None:
        return cached
    return write_cache(
        key,
        call_with_retry(pro.daily, sleep, trade_date=trade_date, fields="ts_code,trade_date,close"),
    )


def financial_indicator_for_code(pro, cfg: Config, ts_code: str) -> pd.DataFrame:
    key = f"fina_indicator_{ts_code}_{cfg.start_date}_{cfg.end_date}"
    cached = read_cache(key)
    if cached is not None:
        return cached

    start_year = int(cfg.start_date[:4]) - 1
    df = call_with_retry(
        pro.fina_indicator,
        cfg.sleep,
        ts_code=ts_code,
        start_date=f"{start_year}0101",
        end_date=cfg.end_date,
        fields="ts_code,ann_date,end_date,roe,netprofit_yoy,ocfps",
    )
    if df.empty:
        df = pd.DataFrame(columns=["ts_code", "ann_date", "end_date", "roe", "netprofit_yoy", "ocfps"])
    return write_cache(key, df.drop_duplicates(["ts_code", "ann_date", "end_date"]))


def financial_indicators_for_codes(pro, cfg: Config, codes: list[str]) -> pd.DataFrame:
    frames = [financial_indicator_for_code(pro, cfg, code) for code in sorted(set(codes))]
    frames = [frame for frame in frames if not frame.empty]
    if not frames:
        return pd.DataFrame(columns=["ts_code", "ann_date", "end_date", "roe", "netprofit_yoy", "ocfps"])
    return pd.concat(frames, ignore_index=True)


def latest_financial_asof(finance: pd.DataFrame, trade_date: str) -> pd.DataFrame:
    available = finance[finance["ann_date"].astype(str) <= trade_date].copy()
    if available.empty:
        return pd.DataFrame(columns=["ts_code", "roe", "netprofit_yoy", "ocfps"])
    available = available.sort_values(["ts_code", "ann_date", "end_date"])
    return available.groupby("ts_code", as_index=False).tail(1)


def pick_stocks(
    pro,
    cfg: Config,
    trade_date: str,
    universe: pd.DataFrame,
    members: pd.DataFrame,
) -> pd.DataFrame:
    listed = universe[
        (pd.to_datetime(trade_date, format=DATE_FMT) - pd.to_datetime(universe["list_date"], format=DATE_FMT)).dt.days
        >= 250
    ]
    panel = (
        daily_basic_on(pro, trade_date, cfg.sleep)
        .merge(listed[["ts_code", "name"]], on="ts_code", how="inner")
        .merge(members, on="ts_code", how="inner")
    )
    panel = panel.replace([np.inf, -np.inf], np.nan)
    panel = panel.dropna(subset=["dv_ttm"])
    panel = panel[panel["dv_ttm"] > 0]
    if panel.empty:
        return panel

    candidates = []
    for _, group in panel.groupby("l1_code"):
        candidates.append(group.sort_values("dv_ttm", ascending=False).head(max(cfg.stocks_per_industry * 4, 5)))
    candidate_panel = pd.concat(candidates, ignore_index=True)

    finance = financial_indicators_for_codes(pro, cfg, candidate_panel["ts_code"].tolist())
    finance_asof = latest_financial_asof(finance, trade_date)
    panel = candidate_panel.merge(finance_asof, on="ts_code", how="inner")
    panel = panel.dropna(subset=["roe", "netprofit_yoy", "ocfps"])
    panel = panel[(panel["roe"] > 0) & (panel["netprofit_yoy"] > 0) & (panel["ocfps"] > 0)]
    if panel.empty:
        return panel

    industry_best = []
    for _, group in panel.groupby("l1_code"):
        industry_best.append(group.sort_values("dv_ttm", ascending=False).head(cfg.stocks_per_industry))
    selected = pd.concat(industry_best, ignore_index=True).sort_values("dv_ttm", ascending=False)
    return selected.head(cfg.target_count).reset_index(drop=True)


def max_drawdown(nav: pd.Series) -> float:
    peak = nav.cummax()
    dd = nav / peak - 1
    return float(dd.min())


def annualized_return(nav: pd.Series, periods_per_year: float = 12.0) -> float:
    if len(nav) < 2:
        return np.nan
    total = nav.iloc[-1] / nav.iloc[0] - 1
    years = (len(nav) - 1) / periods_per_year
    return float((1 + total) ** (1 / years) - 1) if years > 0 else np.nan


def run_backtest(cfg: Config) -> tuple[pd.DataFrame, pd.DataFrame, dict]:
    pro = pro_api()
    dates = trade_dates(pro, cfg)
    rebalances = month_rebalance_dates(dates)
    if len(rebalances) < 3:
        raise RuntimeError("Not enough rebalance dates.")

    universe = stock_universe(pro, cfg.sleep)
    members = industry_members(pro, cfg.sleep)

    bench = call_with_retry(
        pro.index_daily,
        cfg.sleep,
        ts_code=cfg.benchmark,
        start_date=cfg.start_date,
        end_date=cfg.end_date,
        fields="trade_date,close",
    ).sort_values("trade_date")
    bench_close = bench.set_index("trade_date")["close"]

    rows: list[dict] = []
    holdings_rows: list[dict] = []
    strategy_nav = 1.0
    benchmark_nav = 1.0

    for current_date, next_date in zip(rebalances[:-1], rebalances[1:]):
        picks = pick_stocks(pro, cfg, current_date, universe, members)
        start_close = close_on(pro, current_date, cfg.sleep).set_index("ts_code")["close"]
        end_close = close_on(pro, next_date, cfg.sleep).set_index("ts_code")["close"]

        if picks.empty:
            period_return = 0.0
        else:
            picked = picks[picks["ts_code"].isin(start_close.index) & picks["ts_code"].isin(end_close.index)].copy()
            picked["period_return"] = end_close.loc[picked["ts_code"]].to_numpy() / start_close.loc[picked["ts_code"]].to_numpy() - 1
            period_return = float(picked["period_return"].mean()) if not picked.empty else 0.0
            for _, row in picked.iterrows():
                holdings_rows.append(
                    {
                        "trade_date": current_date,
                        "next_date": next_date,
                        "ts_code": row["ts_code"],
                        "name": row["name"],
                        "industry": row["l1_name"],
                        "dv_ttm": row["dv_ttm"],
                        "roe": row["roe"],
                        "netprofit_yoy": row["netprofit_yoy"],
                        "ocfps": row["ocfps"],
                        "period_return": row["period_return"],
                    }
                )

        if current_date in bench_close.index and next_date in bench_close.index:
            bench_return = float(bench_close.loc[next_date] / bench_close.loc[current_date] - 1)
        else:
            bench_return = np.nan

        strategy_nav *= 1 + period_return
        benchmark_nav *= 1 + (0.0 if np.isnan(bench_return) else bench_return)
        rows.append(
            {
                "trade_date": current_date,
                "next_date": next_date,
                "holding_count": int(len(picks)),
                "strategy_return": period_return,
                "benchmark_return": bench_return,
                "excess_return": period_return - bench_return if not np.isnan(bench_return) else np.nan,
                "strategy_nav": strategy_nav,
                "benchmark_nav": benchmark_nav,
            }
        )

    result = pd.DataFrame(rows)
    holdings = pd.DataFrame(holdings_rows)
    monthly = result["strategy_return"]
    excess = result["excess_return"].dropna()
    summary = {
        "start_date": cfg.start_date,
        "end_date": cfg.end_date,
        "benchmark": cfg.benchmark,
        "months": int(len(result)),
        "total_return": float(result["strategy_nav"].iloc[-1] - 1),
        "benchmark_total_return": float(result["benchmark_nav"].iloc[-1] - 1),
        "annualized_return": annualized_return(pd.concat([pd.Series([1.0]), result["strategy_nav"]])),
        "benchmark_annualized_return": annualized_return(pd.concat([pd.Series([1.0]), result["benchmark_nav"]])),
        "max_drawdown": max_drawdown(pd.concat([pd.Series([1.0]), result["strategy_nav"]])),
        "benchmark_max_drawdown": max_drawdown(pd.concat([pd.Series([1.0]), result["benchmark_nav"]])),
        "monthly_win_rate": float((monthly > 0).mean()),
        "excess_win_rate": float((excess > 0).mean()) if not excess.empty else np.nan,
        "monthly_vol": float(monthly.std(ddof=1) * np.sqrt(12)) if len(monthly) > 1 else np.nan,
        "sharpe_zero_rf": float(monthly.mean() / monthly.std(ddof=1) * np.sqrt(12)) if monthly.std(ddof=1) else np.nan,
        "avg_holding_count": float(result["holding_count"].mean()),
        "notes": [
            "Uses Tushare daily_basic.dv_ttm as dividend-yield proxy.",
            "Uses current SW2021 L1 membership from Tushare; this can introduce survivorship/mapping bias.",
            "Ignores transaction costs, slippage, suspensions and intraday limit-up/limit-down execution constraints.",
        ],
    }
    return result, holdings, summary


def parse_args() -> Config:
    parser = argparse.ArgumentParser(description="Backtest high-dividend SW industry equal-weight strategy with Tushare.")
    parser.add_argument("--start-date", default="20190101")
    parser.add_argument("--end-date", default="20260424")
    parser.add_argument("--target-count", type=int, default=10)
    parser.add_argument("--stocks-per-industry", type=int, default=1)
    parser.add_argument("--benchmark", default="000300.SH")
    parser.add_argument("--sleep", type=float, default=0.25)
    args = parser.parse_args()
    return Config(
        start_date=args.start_date,
        end_date=args.end_date,
        target_count=args.target_count,
        stocks_per_industry=args.stocks_per_industry,
        benchmark=args.benchmark,
        sleep=args.sleep,
    )


def main() -> None:
    cfg = parse_args()
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    result, holdings, summary = run_backtest(cfg)
    result.to_csv(OUTPUT_DIR / "monthly_returns.csv", index=False)
    holdings.to_csv(OUTPUT_DIR / "holdings.csv", index=False)
    (OUTPUT_DIR / "summary.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    print(f"Wrote: {OUTPUT_DIR}")


if __name__ == "__main__":
    main()
