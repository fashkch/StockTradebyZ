#!/usr/bin/env python3
"""
StockTradebyZ - ETF股票交易策略系统
主入口程序，整合数据获取和选股功能
"""

import argparse
import json
import logging
import sys
from pathlib import Path
from typing import Optional

import pandas as pd

# 导入项目模块
from fetch_kline import run_fetch_kline
from select_stock import run_select_stock

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("main.log", encoding="utf-8"),
    ],
)
logger = logging.getLogger("main")


class ConfigManager:
    """配置管理器"""
    
    def __init__(self, config_path: str = "project_config.json"):
        self.config_path = Path(config_path)
        self.config = self._load_config()
    
    def _load_config(self) -> dict:
        """加载配置文件"""
        if not self.config_path.exists():
            logger.warning(f"配置文件 {self.config_path} 不存在，使用默认配置")
            return self._get_default_config()
        
        try:
            with open(self.config_path, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception as e:
            logger.error(f"加载配置文件失败: {e}")
            return self._get_default_config()
    
    def _get_default_config(self) -> dict:
        """获取默认配置"""
        return {
            "fetch": {
                "datasource": "mootdx",
                "frequency": 4,
                "exclude_gem": False,
                "min_mktcap": 500000000,
                "max_mktcap": 100000000000000,
                "start": "20250401",
                "end": "today",
                "out": "./data/ETF",
                "workers": 10
            },
            "select": {
                "data_dir": "./data/ETF",
                "config": "./configs.json",
                "date": None,
                "tickers": "all"
            },
            "workflow": {
                "auto_fetch": True,
                "auto_select": True,
                "save_etf_info": True
            }
        }
    
    def get_fetch_args(self) -> argparse.Namespace:
        """获取fetch参数"""
        fetch_config = self.config.get("fetch", {})
        args = argparse.Namespace()
        
        # 设置默认值
        args.datasource = fetch_config.get("datasource", "mootdx")
        args.frequency = fetch_config.get("frequency", 4)
        args.exclude_gem = fetch_config.get("exclude_gem", True)
        args.min_mktcap = fetch_config.get("min_mktcap", 500000000)
        args.max_mktcap = fetch_config.get("max_mktcap")
        args.start = fetch_config.get("start", "20250401")
        args.end = fetch_config.get("end", "today")
        args.out = fetch_config.get("out", "./data/ETF")
        args.workers = fetch_config.get("workers", 10)
        
        return args
    
    def get_select_args(self) -> argparse.Namespace:
        """获取select参数"""
        select_config = self.config.get("select", {})
        args = argparse.Namespace()
        
        # 设置默认值
        args.data_dir = select_config.get("data_dir", "./data/ETF")
        args.config = select_config.get("config", "./configs.json")
        args.date = select_config.get("date")
        args.tickers = select_config.get("tickers", "all")
        args.etf_info = None  # 将在运行时设置
        
        return args


class StockTradeWorkflow:
    """股票交易工作流"""
    
    def __init__(self, config_manager: ConfigManager):
        self.config_manager = config_manager
        self.etf_info_df: Optional[pd.DataFrame] = None
        self.etf_info_path: Optional[Path] = None
    
    def run_fetch(self) -> bool:
        """运行数据获取"""
        logger.info("=" * 50)
        logger.info("开始数据获取阶段")
        logger.info("=" * 50)
        
        try:
            fetch_args = self.config_manager.get_fetch_args()
            self.etf_info_df, out_dir = run_fetch_kline(fetch_args)
            
            if self.etf_info_df is None:
                logger.error("数据获取失败")
                return False
            
            # 保存ETF信息
            if self.config_manager.config["workflow"].get("save_etf_info", True):
                self.etf_info_path = Path(out_dir).parent / "etf_info.csv"
                self.etf_info_df.to_csv(self.etf_info_path, index=False)
                logger.info(f"ETF信息已保存至: {self.etf_info_path}")
            
            logger.info("数据获取完成")
            return True
            
        except Exception as e:
            logger.error(f"数据获取过程中发生错误: {e}")
            return False
    
    def run_select(self) -> bool:
        """运行选股"""
        logger.info("=" * 50)
        logger.info("开始选股阶段")
        logger.info("=" * 50)
        
        try:
            select_args = self.config_manager.get_select_args()
            
            # 设置ETF信息路径
            if self.etf_info_path and self.etf_info_path.exists():
                select_args.etf_info = str(self.etf_info_path)
            
            results = run_select_stock(select_args, self.etf_info_df)
            
            if results is None:
                logger.error("选股失败")
                return False
            
            logger.info("选股完成")
            return True
            
        except Exception as e:
            logger.error(f"选股过程中发生错误: {e}")
            return False
    
    def run(self, fetch_only: bool = False, select_only: bool = False) -> bool:
        """运行完整工作流"""
        logger.info("StockTradebyZ 系统启动")
        logger.info(f"配置文件: {self.config_manager.config_path}")
        
        workflow_config = self.config_manager.config["workflow"]
        
        # 确定执行模式
        if fetch_only:
            should_fetch = True
            should_select = False
        elif select_only:
            should_fetch = False
            should_select = True
        else:
            should_fetch = workflow_config.get("auto_fetch", True)
            should_select = workflow_config.get("auto_select", True)
        
        # 执行数据获取
        if should_fetch:
            if not self.run_fetch():
                return False
        
        # 执行选股
        if should_select:
            if not self.run_select():
                return False
        
        logger.info("=" * 50)
        logger.info("StockTradebyZ 系统运行完成")
        logger.info("=" * 50)
        return True


def main():
    """主函数"""
    parser = argparse.ArgumentParser(
        description="StockTradebyZ - ETF股票交易策略系统",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
使用示例:
  python main.py                    # 运行完整工作流
  python main.py --fetch-only       # 仅执行数据获取
  python main.py --select-only      # 仅执行选股
  python main.py --config my_config.json  # 使用自定义配置文件
        """
    )
    
    parser.add_argument(
        "--config", 
        default="project_config.json",
        help="项目配置文件路径 (默认: project_config.json)"
    )
    parser.add_argument(
        "--fetch-only", 
        action="store_true",
        help="仅执行数据获取阶段"
    )
    parser.add_argument(
        "--select-only", 
        action="store_true",
        help="仅执行选股阶段"
    )
    
    args = parser.parse_args()
    
    # 检查参数冲突
    if args.fetch_only and args.select_only:
        logger.error("--fetch-only 和 --select-only 不能同时使用")
        sys.exit(1)
    
    try:
        # 初始化配置管理器
        config_manager = ConfigManager(args.config)
        
        # 创建工作流实例
        workflow = StockTradeWorkflow(config_manager)
        
        # 运行工作流
        success = workflow.run(
            fetch_only=args.fetch_only,
            select_only=args.select_only
        )
        
        if not success:
            logger.error("工作流执行失败")
            sys.exit(1)
            
    except KeyboardInterrupt:
        logger.info("用户中断执行")
        sys.exit(0)
    except Exception as e:
        logger.error(f"程序执行出错: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
