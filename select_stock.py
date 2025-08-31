from __future__ import annotations

import argparse
import importlib
import json
import logging
import sys
from pathlib import Path
from typing import Any, Dict, Iterable, List

import pandas as pd

# ---------- 日志 ----------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        # 将日志写入文件
        logging.FileHandler("select_results.log", encoding="utf-8"),
    ],
)
logger = logging.getLogger("select")


# ---------- 工具 ----------

def load_data(data_dir: Path, codes: Iterable[str]) -> Dict[str, pd.DataFrame]:
    frames: Dict[str, pd.DataFrame] = {}
    for code in codes:
        fp = data_dir / f"{code}.csv"
        if not fp.exists():
            logger.warning("%s 不存在，跳过", fp.name)
            continue
        df = pd.read_csv(fp, parse_dates=["date"]).sort_values("date")
        frames[code] = df
    return frames


def load_config(cfg_path: Path) -> List[Dict[str, Any]]:
    if not cfg_path.exists():
        logger.error("配置文件 %s 不存在", cfg_path)
        sys.exit(1)
    with cfg_path.open(encoding="utf-8") as f:
        cfg_raw = json.load(f)

    # 兼容三种结构：单对象、对象数组、或带 selectors 键
    if isinstance(cfg_raw, list):
        cfgs = cfg_raw
    elif isinstance(cfg_raw, dict) and "selectors" in cfg_raw:
        cfgs = cfg_raw["selectors"]
    else:
        cfgs = [cfg_raw]

    if not cfgs:
        logger.error("configs.json 未定义任何 Selector")
        sys.exit(1)

    return cfgs


def instantiate_selector(cfg: Dict[str, Any]):
    """动态加载 Selector 类并实例化"""
    cls_name: str = cfg.get("class")
    if not cls_name:
        raise ValueError("缺少 class 字段")

    try:
        module = importlib.import_module("Selector")
        cls = getattr(module, cls_name)
    except (ModuleNotFoundError, AttributeError) as e:
        raise ImportError(f"无法加载 Selector.{cls_name}: {e}") from e

    params = cfg.get("params", {})
    return cfg.get("alias", cls_name), cls(**params)


# ---------- 主函数 ----------

def main():
    p = argparse.ArgumentParser(description="Run selectors defined in configs.json")
    p.add_argument("--data-dir", default="./data/ETF", help="CSV 行情目录")
    p.add_argument("--config", default="./configs.json", help="Selector 配置文件")
    p.add_argument("--date", help="交易日 YYYY-MM-DD；缺省=数据最新日期")
    p.add_argument("--tickers", default="all", help="'all' 或逗号分隔股票代码列表")
    p.add_argument("--etf-info", help="ETF信息文件路径，用于显示详细信息")
    args = p.parse_args()

    return run_select_stock(args)

def run_select_stock(args, etf_info_df=None):
    """可被其他模块调用的函数"""
    # --- 加载行情 ---
    data_dir = Path(args.data_dir)
    if not data_dir.exists():
        logger.error("数据目录 %s 不存在", data_dir)
        return None

    codes = (
        [f.stem for f in data_dir.glob("*.csv")]
        if args.tickers.lower() == "all"
        else [c.strip() for c in args.tickers.split(",") if c.strip()]
    )
    if not codes:
        logger.error("股票池为空！")
        return None

    data = load_data(data_dir, codes)
    if not data:
        logger.error("未能加载任何行情数据")
        return None

    trade_date = (
        pd.to_datetime(args.date)
        if args.date
        else max(df["date"].max() for df in data.values())
    )
    if not args.date:
        logger.info("未指定 --date，使用最近日期 %s", trade_date.date())

    # --- 加载 Selector 配置 ---
    selector_cfgs = load_config(Path(args.config))

    # --- 加载ETF信息 ---
    if etf_info_df is None and args.etf_info:
        try:
            etf_info_df = pd.read_csv(args.etf_info)
        except Exception as e:
            logger.warning("无法加载ETF信息文件: %s", e)

    # --- 逐个 Selector 运行 ---
    results = {}
    for cfg in selector_cfgs:
        if cfg.get("activate", True) is False:
            continue
        try:
            alias, selector = instantiate_selector(cfg)
        except Exception as e:
            logger.error("跳过配置 %s：%s", cfg, e)
            continue

        picks = selector.select(trade_date, data)
        results[alias] = picks

        # 将结果写入日志，同时输出到控制台
        logger.info("")
        logger.info("============== 选股结果 [%s] ==============", alias)
        logger.info("交易日: %s", trade_date.date())
        logger.info("符合条件股票数: %d", len(picks))
        
        # 显示详细信息
        if picks and etf_info_df is not None:
            detailed_picks = []
            for code in picks:
                etf_info = etf_info_df[etf_info_df['code'] == code]
                if not etf_info.empty:
                    row = etf_info.iloc[0]
                    name = row.get('名称', '未知')
                    mktcap = row.get('mktcap', 0)
                    price = row.get('最新价', 0)
                    change_pct = row.get('涨跌幅', 0)
                    
                    # 构建详细信息字符串
                    info_parts = [f"{code} ({name})"]
                    
                    if not pd.isna(mktcap) and mktcap > 0:
                        info_parts.append(f"市值:{mktcap/1e8:.2f}亿")
                    
                    if not pd.isna(price) and price > 0:
                        info_parts.append(f"价格:{price:.3f}")
                    
                    if not pd.isna(change_pct):
                        info_parts.append(f"涨跌:{change_pct:+.2f}%")
                    logger.info(" --  %s", info_parts)
                    # detailed_picks.append(" ".join(info_parts))
                else:
                    detailed_picks.append(code)
            logger.info("详细信息缺失: %s", ", ".join(detailed_picks) if detailed_picks else "无")
        else:
            logger.info("选股详情: %s", ", ".join(picks) if picks else "无符合条件股票")
    
    return results


if __name__ == "__main__":
    main()
