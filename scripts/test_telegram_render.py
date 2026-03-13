# -*- coding: utf-8 -*-
"""
Render a fixed Markdown sample to PNG and optionally send it to Telegram.

Used by a lightweight GitHub Actions workflow to iterate on Telegram image
rendering without running the full stock analysis pipeline.
"""

from __future__ import annotations

import os
from pathlib import Path

from src.config import get_config, setup_env
from src.md2img import markdown_to_image
from src.notification_sender.telegram_sender import TelegramSender


SAMPLE_MARKDOWN = """# 🎯 2026-03-14

## 📊 市场摘要

- 上涨：1502 家
- 下跌：3829 家
- 成交额：24173 亿元

## 📈 指数表现

| 指数 | 点位 | 涨跌幅 | 成交额(亿) |
| --- | ---: | ---: | ---: |
| 上证指数 | 4095.45 | -0.81% | 10639 |
| 深证成指 | 14280.78 | -0.66% | 13364 |
| 创业板指 | 3310.28 | -0.22% | 5764 |

## 🚀 小米集团-W（HK01810）

- 当前价：33.32
- MA5：33.452
- MA10：32.96
- MA20：34.503

### ✅ 检查清单

- ✅ 乖离率安全：-0.39%
- ⚠️ 未形成多头排列
- ⚠️ 港股实时数据字段存在缺失
- ✅ Telegram 图片模式测试

> 这是一条中文、emoji、表格混合渲染测试消息。
"""


def main() -> int:
    setup_env()
    config = get_config()

    output_dir = Path("render_test_output")
    output_dir.mkdir(parents=True, exist_ok=True)
    md_path = output_dir / "sample.md"
    png_path = output_dir / "sample.png"
    md_path.write_text(SAMPLE_MARKDOWN, encoding="utf-8")

    max_chars = int(getattr(config, "markdown_to_image_max_chars", 15000) or 15000)
    image_bytes = markdown_to_image(SAMPLE_MARKDOWN, max_chars=max_chars)
    if not image_bytes:
        raise RuntimeError("Markdown to image conversion returned no image bytes.")

    png_path.write_bytes(image_bytes)
    print(f"Rendered PNG: {png_path}")
    print(f"PNG size: {len(image_bytes)} bytes")

    if os.getenv("SEND_TO_TELEGRAM", "false").lower() == "true":
        sender = TelegramSender(config)
        if not sender._send_telegram_photo(image_bytes):
            raise RuntimeError("Telegram image send failed.")
        print("Telegram image send succeeded.")
    else:
        print("SEND_TO_TELEGRAM=false, skipped Telegram send.")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
