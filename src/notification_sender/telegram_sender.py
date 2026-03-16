# -*- coding: utf-8 -*-
"""
Telegram sender.

Responsibilities:
1. Send text messages via Telegram Bot API.
2. Send rendered report images as files to preserve clarity.
"""

from __future__ import annotations

import logging
import re
import time
from typing import Optional

import requests

from src.config import Config


logger = logging.getLogger(__name__)


class TelegramSender:
    def __init__(self, config: Config):
        self._telegram_config = {
            "bot_token": getattr(config, "telegram_bot_token", None),
            "chat_id": getattr(config, "telegram_chat_id", None),
            "message_thread_id": getattr(config, "telegram_message_thread_id", None),
        }

    def _is_telegram_configured(self) -> bool:
        return bool(
            self._telegram_config["bot_token"] and self._telegram_config["chat_id"]
        )

    def send_to_telegram(self, content: str) -> bool:
        if not self._is_telegram_configured():
            logger.warning("Telegram config incomplete, skipping notification")
            return False

        bot_token = self._telegram_config["bot_token"]
        chat_id = self._telegram_config["chat_id"]
        message_thread_id = self._telegram_config.get("message_thread_id")

        try:
            api_url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
            max_length = 4096

            if len(content) <= max_length:
                return self._send_telegram_message(
                    api_url, chat_id, content, message_thread_id
                )
            return self._send_telegram_chunked(
                api_url, chat_id, content, max_length, message_thread_id
            )
        except Exception as e:
            logger.error("Telegram text send failed: %s", e)
            logger.debug("Telegram text send traceback", exc_info=True)
            return False

    def _send_telegram_message(
        self,
        api_url: str,
        chat_id: str,
        text: str,
        message_thread_id: Optional[str] = None,
    ) -> bool:
        telegram_text = self._convert_to_telegram_markdown(text)

        payload = {
            "chat_id": chat_id,
            "text": telegram_text,
            "parse_mode": "Markdown",
            "disable_web_page_preview": True,
        }

        if message_thread_id:
            payload["message_thread_id"] = message_thread_id

        max_retries = 3
        for attempt in range(1, max_retries + 1):
            try:
                response = requests.post(api_url, json=payload, timeout=10)
            except (requests.exceptions.ConnectionError, requests.exceptions.Timeout) as e:
                if attempt < max_retries:
                    delay = 2 ** attempt
                    logger.warning(
                        "Telegram request failed (attempt %s/%s): %s, retrying in %ss...",
                        attempt,
                        max_retries,
                        e,
                        delay,
                    )
                    time.sleep(delay)
                    continue
                logger.error("Telegram request failed after %s attempts: %s", max_retries, e)
                return False

            if response.status_code == 200:
                result = response.json()
                if result.get("ok"):
                    logger.info("Telegram message sent successfully")
                    return True

                error_desc = result.get("description", "unknown error")
                logger.error("Telegram returned error: %s", error_desc)

                if "parse" in error_desc.lower() or "markdown" in error_desc.lower():
                    logger.info("Retrying Telegram send as plain text...")
                    plain_payload = dict(payload)
                    plain_payload.pop("parse_mode", None)
                    plain_payload["text"] = text
                    try:
                        response = requests.post(api_url, json=plain_payload, timeout=10)
                        if response.status_code == 200 and response.json().get("ok"):
                            logger.info("Telegram plain-text fallback sent successfully")
                            return True
                    except (requests.exceptions.ConnectionError, requests.exceptions.Timeout) as e:
                        logger.error("Telegram plain-text fallback failed: %s", e)

                return False

            if response.status_code == 429:
                retry_after = int(response.headers.get("Retry-After", 2 ** attempt))
                if attempt < max_retries:
                    logger.warning(
                        "Telegram rate limited, retrying in %ss (attempt %s/%s)...",
                        retry_after,
                        attempt,
                        max_retries,
                    )
                    time.sleep(retry_after)
                    continue
                logger.error("Telegram rate limited after %s attempts", max_retries)
                return False

            if attempt < max_retries and response.status_code >= 500:
                delay = 2 ** attempt
                logger.warning(
                    "Telegram server error HTTP %s (attempt %s/%s), retrying in %ss...",
                    response.status_code,
                    attempt,
                    max_retries,
                    delay,
                )
                time.sleep(delay)
                continue

            logger.error("Telegram request failed: HTTP %s", response.status_code)
            logger.error("Telegram response: %s", response.text)
            return False

        return False

    def _send_telegram_chunked(
        self,
        api_url: str,
        chat_id: str,
        content: str,
        max_length: int,
        message_thread_id: Optional[str] = None,
    ) -> bool:
        sections = content.split("\n---\n")
        current_chunk = []
        current_length = 0
        all_success = True
        chunk_index = 1

        for section in sections:
            section_length = len(section) + 5

            if current_length + section_length > max_length:
                if current_chunk:
                    chunk_content = "\n---\n".join(current_chunk)
                    logger.info("Sending Telegram chunk %s...", chunk_index)
                    if not self._send_telegram_message(
                        api_url, chat_id, chunk_content, message_thread_id
                    ):
                        all_success = False
                    chunk_index += 1

                current_chunk = [section]
                current_length = section_length
            else:
                current_chunk.append(section)
                current_length += section_length

        if current_chunk:
            chunk_content = "\n---\n".join(current_chunk)
            logger.info("Sending Telegram chunk %s...", chunk_index)
            if not self._send_telegram_message(
                api_url, chat_id, chunk_content, message_thread_id
            ):
                all_success = False

        return all_success

    def _send_telegram_photo(self, image_bytes: bytes) -> bool:
        """Send image as Telegram document to avoid platform photo compression."""
        if not self._is_telegram_configured():
            return False

        bot_token = self._telegram_config["bot_token"]
        chat_id = self._telegram_config["chat_id"]
        message_thread_id = self._telegram_config.get("message_thread_id")
        api_url = f"https://api.telegram.org/bot{bot_token}/sendDocument"

        try:
            data = {"chat_id": chat_id}
            if message_thread_id:
                data["message_thread_id"] = message_thread_id
            files = {"document": ("report.png", image_bytes, "image/png")}
            response = requests.post(api_url, data=data, files=files, timeout=60)
            if response.status_code == 200 and response.json().get("ok"):
                logger.info("Telegram document sent successfully")
                return True
            logger.error("Telegram document send failed: %s", response.text[:200])
            return False
        except Exception as e:
            logger.error("Telegram document send exception: %s", e)
            return False

    def _convert_to_telegram_markdown(self, text: str) -> str:
        result = text
        result = re.sub(r"^#{1,6}\s+", "", result, flags=re.MULTILINE)
        result = re.sub(r"\*\*(.+?)\*\*", r"*\1*", result)

        import uuid as _uuid

        link_placeholder = f"__LINK_{_uuid.uuid4().hex[:8]}__"
        links = []

        def _save_link(match: re.Match[str]) -> str:
            links.append(match.group(0))
            return f"{link_placeholder}{len(links) - 1}"

        result = re.sub(r"\[([^\]]+)\]\(([^)]+)\)", _save_link, result)

        for char in ["[", "]", "(", ")"]:
            result = result.replace(char, f"\\{char}")

        for i, link in enumerate(links):
            result = result.replace(f"{link_placeholder}{i}", link)

        return result
