# High Dividend Industry Equal Weight Strategy

一个基于 Tushare 的 A 股高股息行业均仓策略回测项目。

策略思路很朴素：每月在申万一级行业内寻找高股息且财务质量为正的股票，每个行业先选代表股，再从候选股中选择股息率最高的 10 只等权持有。

## Strategy Logic

月度调仓流程：

1. 获取 A 股上市股票池，剔除 ST、退市、新股和部分不适合交易的市场。
2. 使用申万 2021 一级行业分类进行行业归属映射。
3. 使用 `daily_basic.dv_ttm` 作为股息率代理指标。
4. 使用最新已披露财务指标过滤：
   - ROE > 0
   - 归母净利润同比增速 > 0
   - 每股经营现金流 > 0
5. 每个申万一级行业保留高股息候选股。
6. 全市场按股息率排序，选择前 10 只等权持有至下月第一个交易日。

## Backtest Snapshot

回测区间：2019-01-01 至 2026-04-24  
基准：沪深300 `000300.SH`  
数据源：Tushare Pro

| Metric | Strategy | CSI 300 |
|---|---:|---:|
| Total Return | 361.1% | 52.4% |
| Annualized Return | 23.5% | 6.0% |
| Max Drawdown | -17.0% | -40.6% |
| Monthly Win Rate | 58.6% | - |
| Excess Monthly Win Rate | 55.2% | - |

年度收益：

| Year | Strategy | CSI 300 |
|---|---:|---:|
| 2019 | 24.8% | 39.8% |
| 2020 | 12.3% | 26.9% |
| 2021 | 59.3% | -6.6% |
| 2022 | -0.8% | -20.9% |
| 2023 | 46.7% | -12.9% |
| 2024 | 13.4% | 12.8% |
| 2025 | 17.3% | 23.5% |
| 2026 YTD | 6.7% | -4.1% |

## Run

Install dependencies:

```bash
pip install -r requirements.txt
```

Set your Tushare token locally:

```bash
cp .env.example .env
```

Then edit `.env` and fill in your own token. The `.env` file is ignored by Git and should never be committed.

Run the backtest:

```bash
python backtest_tushare.py --start-date 20190101 --end-date 20260424
```

Generated raw/cache data is stored under `data/`, and detailed outputs are stored under `outputs/`. These files are ignored by default except the summary snapshot.

## Files

| File | Purpose |
|---|---|
| `backtest_tushare.py` | Main Tushare data fetcher and monthly backtest engine |
| `deepseek_review.py` | Optional DeepSeek-powered strategy review generator |
| `requirements.txt` | Python dependencies |
| `.env.example` | Local token template without any real credential |
| `outputs/summary.json` | Public, aggregate backtest summary |

## DeepSeek Review

This project can optionally use DeepSeek's OpenAI-compatible API to generate a concise strategy review from `outputs/summary.json`.

Add your local API key to `.env`:

```bash
DEEPSEEK_API_KEY=your_deepseek_api_key_here
DEEPSEEK_MODEL=deepseek-chat
```

Then run:

```bash
python deepseek_review.py
```

The generated review is written to `outputs/deepseek_review.md`, which is ignored by Git by default.

If DeepSeek changes the public model name, set `DEEPSEEK_MODEL` in `.env` or pass `--model` on the command line.

## Important Notes

This is a research prototype, not investment advice.

Current limitations:

- `daily_basic.dv_ttm` is used as a dividend-yield proxy, not a full three-year cash-dividend reconstruction.
- The industry mapping uses current Tushare SW2021 membership, so historical industry membership bias may exist.
- The backtest does not yet model suspensions, limit-up/limit-down execution failure, slippage, taxes, or full turnover-based transaction costs.
- Raw Tushare data and local credentials are intentionally excluded from the repository.
