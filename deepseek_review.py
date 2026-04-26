from __future__ import annotations

import argparse
import json
import os
from pathlib import Path

from openai import OpenAI


ROOT = Path(__file__).resolve().parent
DEFAULT_MODEL = os.getenv("DEEPSEEK_MODEL", "deepseek-chat")


def read_env_value(key: str) -> str | None:
    value = os.getenv(key)
    if value:
        return value

    env_path = ROOT / ".env"
    if not env_path.exists():
        return None

    for line in env_path.read_text(encoding="utf-8").splitlines():
        if line.startswith(f"{key}="):
            return line.split("=", 1)[1].strip()
    return None


def load_summary(path: Path) -> dict:
    if not path.exists():
        raise FileNotFoundError(f"Summary file not found: {path}")
    return json.loads(path.read_text(encoding="utf-8"))


def build_prompt(summary: dict) -> str:
    return f"""
你是一名量化策略研究员。请基于下面的回测摘要，生成一份简洁的中文策略点评。

要求：
1. 解释这个高股息行业均仓策略为什么可能有效。
2. 指出最重要的 3-5 个风险或回测偏差。
3. 给出下一步验证建议。
4. 不要提供具体买卖建议，不要把历史收益说成未来保证。

回测摘要 JSON：
{json.dumps(summary, ensure_ascii=False, indent=2)}
""".strip()


def run_review(summary_path: Path, model: str) -> str:
    api_key = read_env_value("DEEPSEEK_API_KEY")
    if not api_key:
        raise RuntimeError("Missing DEEPSEEK_API_KEY. Set it in .env or environment variables.")

    client = OpenAI(api_key=api_key, base_url="https://api.deepseek.com")
    summary = load_summary(summary_path)
    response = client.chat.completions.create(
        model=model,
        messages=[
            {"role": "system", "content": "你是严谨、克制的量化策略研究员。"},
            {"role": "user", "content": build_prompt(summary)},
        ],
        temperature=0.2,
    )
    return response.choices[0].message.content or ""


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate a DeepSeek review for the backtest summary.")
    parser.add_argument("--summary", default=str(ROOT / "outputs" / "summary.json"))
    parser.add_argument("--model", default=DEFAULT_MODEL)
    parser.add_argument("--output", default=str(ROOT / "outputs" / "deepseek_review.md"))
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    text = run_review(Path(args.summary), args.model)
    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(text, encoding="utf-8")
    print(f"Wrote: {output_path}")


if __name__ == "__main__":
    main()
