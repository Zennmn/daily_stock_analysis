# -*- coding: utf-8 -*-
"""
Stock data service layer.
"""

import logging
from datetime import datetime
from typing import Any, Dict, List, Optional

import pandas as pd

from src.repositories.stock_repo import StockRepository

logger = logging.getLogger(__name__)


class StockService:
    """Service wrapper for stock quote, history, and recommendation logic."""

    def __init__(self):
        self.repo = StockRepository()

    def get_realtime_quote(self, stock_code: str) -> Optional[Dict[str, Any]]:
        """Get a realtime quote for one stock."""
        try:
            from data_provider.base import DataFetcherManager

            manager = DataFetcherManager()
            quote = manager.get_realtime_quote(stock_code)

            if quote is None:
                logger.warning("get realtime quote failed: %s", stock_code)
                return None

            return {
                "stock_code": getattr(quote, "code", stock_code),
                "stock_name": getattr(quote, "name", None),
                "current_price": getattr(quote, "price", 0.0) or 0.0,
                "change": getattr(quote, "change_amount", None),
                "change_percent": getattr(quote, "change_pct", None),
                "open": getattr(quote, "open_price", None),
                "high": getattr(quote, "high", None),
                "low": getattr(quote, "low", None),
                "prev_close": getattr(quote, "pre_close", None),
                "volume": getattr(quote, "volume", None),
                "amount": getattr(quote, "amount", None),
                "update_time": datetime.now().isoformat(),
            }
        except ImportError:
            logger.warning("DataFetcherManager not found, fallback to placeholder quote")
            return self._get_placeholder_quote(stock_code)
        except Exception as e:
            logger.error("get realtime quote failed: %s", e, exc_info=True)
            return None

    def get_history_data(
        self,
        stock_code: str,
        period: str = "daily",
        days: int = 30,
    ) -> Dict[str, Any]:
        """Get historical daily data for one stock."""
        if period != "daily":
            raise ValueError(
                f"unsupported period '{period}', currently only 'daily' is supported"
            )

        try:
            from data_provider.base import DataFetcherManager

            manager = DataFetcherManager()
            df, _source = manager.get_daily_data(stock_code, days=days)

            if df is None or df.empty:
                logger.warning("get history data failed: %s", stock_code)
                return {"stock_code": stock_code, "period": period, "data": []}

            stock_name = manager.get_stock_name(stock_code)
            data = []
            for _, row in df.iterrows():
                date_val = row.get("date")
                if hasattr(date_val, "strftime"):
                    date_str = date_val.strftime("%Y-%m-%d")
                else:
                    date_str = str(date_val)

                data.append(
                    {
                        "date": date_str,
                        "open": float(row.get("open", 0)),
                        "high": float(row.get("high", 0)),
                        "low": float(row.get("low", 0)),
                        "close": float(row.get("close", 0)),
                        "volume": float(row.get("volume", 0)) if row.get("volume") else None,
                        "amount": float(row.get("amount", 0)) if row.get("amount") else None,
                        "change_percent": float(row.get("pct_chg", 0)) if row.get("pct_chg") else None,
                    }
                )

            return {
                "stock_code": stock_code,
                "stock_name": stock_name,
                "period": period,
                "data": data,
            }
        except ImportError:
            logger.warning("DataFetcherManager not found, return empty history")
            return {"stock_code": stock_code, "period": period, "data": []}
        except Exception as e:
            logger.error("get history data failed: %s", e, exc_info=True)
            return {"stock_code": stock_code, "period": period, "data": []}

    def recommend_stocks(self, limit: int = 3, market: str = "cn") -> List[Dict[str, Any]]:
        """Return momentum-style stock recommendations for cn / hk / us markets."""
        try:
            import akshare as ak
        except ImportError:
            logger.warning("akshare not installed, recommendation feature unavailable")
            return []

        market_key = (market or "cn").strip().lower()
        try:
            if market_key == "hk":
                df = self._load_hk_market_snapshot(ak)
            elif market_key == "us":
                df = self._load_us_market_snapshot(ak)
            else:
                df = self._load_cn_market_snapshot(ak)
        except Exception as e:
            logger.error(
                "load recommendation snapshot failed: market=%s err=%s",
                market_key,
                e,
                exc_info=True,
            )
            return []

        if df is None or df.empty:
            return []

        candidates = self._build_recommendation_candidates(df, market=market_key)
        return candidates[: max(1, limit)]

    def _load_cn_market_snapshot(self, ak: Any) -> pd.DataFrame:
        try:
            return ak.stock_zh_a_spot_em()
        except Exception as primary_error:
            logger.warning("stock_zh_a_spot_em failed, fallback to ranked candidates: %s", primary_error)

        candidate_frames: List[pd.DataFrame] = []
        fallback_loaders = [
            ("stock_hot_rank_em", self._load_cn_hot_rank_snapshot),
            ("stock_hot_up_em", self._load_cn_hot_up_snapshot),
        ]
        for loader_name, loader in fallback_loaders:
            try:
                fallback_df = loader(ak)
                if fallback_df is not None and not fallback_df.empty:
                    candidate_frames.append(fallback_df)
            except Exception as e:
                logger.warning("%s fallback failed: %s", loader_name, e)

        if not candidate_frames:
            raise ConnectionError("all cn recommendation snapshot loaders failed")

        merged = pd.concat(candidate_frames, ignore_index=True, sort=False)
        if "代码" in merged.columns:
            merged = merged.drop_duplicates(subset=["代码"])
        elif "stock_code" in merged.columns:
            merged = merged.drop_duplicates(subset=["stock_code"])
        return merged

    def _load_hk_market_snapshot(self, ak: Any) -> pd.DataFrame:
        try:
            return ak.stock_hk_main_board_spot_em()
        except Exception:
            return ak.stock_hk_spot_em()

    def _load_us_market_snapshot(self, ak: Any) -> pd.DataFrame:
        return ak.stock_us_spot_em()

    def _load_cn_hot_rank_snapshot(self, ak: Any) -> pd.DataFrame:
        raw = ak.stock_hot_rank_em()
        if raw is None or raw.empty:
            return pd.DataFrame()

        code_col = self._pick_existing_column(raw, ["代码", "股票代码"])
        name_col = self._pick_existing_column(raw, ["股票名称", "名称"])
        if not code_col or not name_col:
            return pd.DataFrame()

        fallback = raw.rename(columns={code_col: "代码", name_col: "名称"}).copy()
        fallback["最新价"] = pd.to_numeric(fallback.get("最新价"), errors="coerce")
        fallback["涨跌幅"] = pd.to_numeric(fallback.get("涨跌幅"), errors="coerce")
        fallback["换手率"] = pd.to_numeric(fallback.get("换手率"), errors="coerce")
        fallback["量比"] = pd.to_numeric(fallback.get("量比"), errors="coerce")
        return fallback

    def _load_cn_hot_up_snapshot(self, ak: Any) -> pd.DataFrame:
        candidate_functions = [
            "stock_hot_up_em",
            "stock_zt_pool_em",
            "stock_zt_pool_dtgc_em",
        ]
        for func_name in candidate_functions:
            func = getattr(ak, func_name, None)
            if not callable(func):
                continue
            raw = func()
            if raw is None or raw.empty:
                continue
            code_col = self._pick_existing_column(raw, ["代码", "股票代码"])
            name_col = self._pick_existing_column(raw, ["名称", "股票名称"])
            if not code_col or not name_col:
                continue
            fallback = raw.rename(columns={code_col: "代码", name_col: "名称"}).copy()
            fallback["最新价"] = pd.to_numeric(fallback.get("最新价"), errors="coerce")
            fallback["涨跌幅"] = pd.to_numeric(fallback.get("涨跌幅"), errors="coerce")
            fallback["换手率"] = pd.to_numeric(fallback.get("换手率"), errors="coerce")
            fallback["量比"] = pd.to_numeric(fallback.get("量比"), errors="coerce")
            return fallback
        return pd.DataFrame()

    def _build_recommendation_candidates(
        self,
        df: pd.DataFrame,
        market: str = "cn",
    ) -> List[Dict[str, Any]]:
        working = df.copy()

        code_col = self._pick_existing_column(working, ["代码", "股票代码", "symbol", "代码"])
        name_col = self._pick_existing_column(working, ["名称", "股票名称", "name"])
        price_col = self._pick_existing_column(working, ["最新价", "现价", "price"])
        pct_col = self._pick_existing_column(working, ["涨跌幅", "涨幅", "change_percent"])
        turnover_col = self._pick_existing_column(working, ["换手率", "turnover_rate"])
        volume_ratio_col = self._pick_existing_column(working, ["量比", "volume_ratio"])
        market_cap_col = self._pick_existing_column(working, ["总市值", "总市值-动态", "market_cap"])

        if not all([code_col, name_col, price_col, pct_col]):
            logger.warning(
                "recommendation columns missing: market=%s available=%s",
                market,
                list(working.columns),
            )
            return []

        rename_map = {
            code_col: "stock_code",
            name_col: "stock_name",
            price_col: "current_price",
            pct_col: "change_percent",
        }
        if turnover_col:
            rename_map[turnover_col] = "turnover_rate"
        if volume_ratio_col:
            rename_map[volume_ratio_col] = "volume_ratio"
        if market_cap_col:
            rename_map[market_cap_col] = "market_cap"
        working = working.rename(columns=rename_map)

        for col in ["current_price", "change_percent", "turnover_rate", "volume_ratio", "market_cap"]:
            if col in working.columns:
                working[col] = pd.to_numeric(working[col], errors="coerce")

        working["stock_code"] = working["stock_code"].astype(str).str.strip()
        working["stock_name"] = working["stock_name"].astype(str).str.strip()
        working["stock_code"] = working["stock_code"].apply(
            lambda value: self._normalize_recommendation_code(value, market)
        )

        filtered = self._filter_recommendation_universe(working, market)
        if filtered.empty:
            return []

        if "turnover_rate" not in filtered.columns:
            filtered["turnover_rate"] = 0.0
        if "volume_ratio" not in filtered.columns:
            filtered["volume_ratio"] = 1.0
        if "market_cap" not in filtered.columns:
            filtered["market_cap"] = None

        filtered["market_cap_score"] = filtered["market_cap"].apply(self._market_cap_score)
        filtered["score"] = (
            filtered["change_percent"].fillna(0) * 0.42
            + filtered["volume_ratio"].fillna(0) * 16
            + filtered["turnover_rate"].fillna(0) * 1.5
            + filtered["market_cap_score"].fillna(0)
        )
        filtered = filtered.sort_values(
            ["score", "change_percent", "turnover_rate"],
            ascending=False,
        )

        items: List[Dict[str, Any]] = []
        for _, row in filtered.head(max(10, 3)).iterrows():
            pct = self._safe_round(row.get("change_percent"))
            turnover = self._safe_round(row.get("turnover_rate"))
            volume_ratio = self._safe_round(row.get("volume_ratio"))
            reason_parts = [
                f"momentum {pct}%" if pct is not None else None,
                f"volume_ratio {volume_ratio}" if volume_ratio is not None and volume_ratio >= 1.2 else None,
                f"turnover {turnover}%" if turnover is not None and turnover >= 2 else None,
            ]
            items.append(
                {
                    "stock_code": str(row.get("stock_code")),
                    "stock_name": str(row.get("stock_name")),
                    "current_price": self._safe_round(row.get("current_price")),
                    "change_percent": pct,
                    "turnover_rate": turnover,
                    "volume_ratio": volume_ratio,
                    "score": round(float(row.get("score", 0.0)), 2),
                    "reason": ", ".join([part for part in reason_parts if part])
                    or "strong momentum and active trading",
                }
            )
        return items

    def _filter_recommendation_universe(
        self,
        working: pd.DataFrame,
        market: str,
    ) -> pd.DataFrame:
        if market == "hk":
            return working[
                working["stock_code"].str.match(r"^HK\d{5}$", na=False)
                & ~working["stock_name"].str.upper().str.contains("ST", na=False)
                & (working["current_price"] >= 1)
                & working["change_percent"].between(1.5, 18.0, inclusive="both")
            ].copy()

        if market == "us":
            return working[
                working["stock_code"].str.match(r"^[A-Z]{1,5}(\.[A-Z])?$", na=False)
                & ~working["stock_name"].str.upper().str.contains("ETF", na=False)
                & (working["current_price"] >= 3)
                & working["change_percent"].between(1.5, 20.0, inclusive="both")
            ].copy()

        return working[
            working["stock_code"].str.match(r"^\d{6}$", na=False)
            & ~working["stock_name"].str.upper().str.contains("ST", na=False)
            & (working["current_price"] >= 3)
            & working["change_percent"].between(2.0, 9.7, inclusive="both")
        ].copy()

    @staticmethod
    def _pick_existing_column(df: pd.DataFrame, candidates: List[str]) -> Optional[str]:
        for name in candidates:
            if name in df.columns:
                return name
        return None

    @staticmethod
    def _market_cap_score(value: Any) -> float:
        try:
            market_cap = float(value)
        except (TypeError, ValueError):
            return 0.0

        if market_cap <= 0:
            return 0.0
        if market_cap <= 8_000_000_000:
            return 12.0
        if market_cap <= 20_000_000_000:
            return 8.0
        if market_cap <= 50_000_000_000:
            return 4.0
        return 1.0

    @staticmethod
    def _safe_round(value: Any, digits: int = 2) -> Optional[float]:
        try:
            return round(float(value), digits)
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _normalize_recommendation_code(raw_code: Any, market: str) -> str:
        text = str(raw_code or "").strip().upper()
        if not text:
            return ""

        if market == "hk":
            digits = "".join(ch for ch in text if ch.isdigit())
            if 1 <= len(digits) <= 5:
                return f"HK{digits.zfill(5)}"
            return text

        if market == "us":
            if "." in text:
                text = text.rsplit(".", 1)[-1]
            return text

        return text

    def _get_placeholder_quote(self, stock_code: str) -> Dict[str, Any]:
        """Return a placeholder quote for tests and fallback flows."""
        return {
            "stock_code": stock_code,
            "stock_name": f"股票{stock_code}",
            "current_price": 0.0,
            "change": None,
            "change_percent": None,
            "open": None,
            "high": None,
            "low": None,
            "prev_close": None,
            "volume": None,
            "amount": None,
            "update_time": datetime.now().isoformat(),
        }
