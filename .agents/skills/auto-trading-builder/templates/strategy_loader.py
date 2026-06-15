"""
전략 동적 로더 (StrategyLoader)
────────────────────────────────
strategies/ 디렉토리의 .py 파일을 importlib로 동적 로딩하여
UI 목록 제공 및 CompositeStrategy 조립에 활용한다.

디렉토리 구조 예시:
  strategies/
    rsi_strategy.py
    macd_strategy.py
    disparity_strategy.py
    bollinger_strategy.py
"""

import importlib.util
import inspect
from pathlib import Path
from typing import Dict, List, Optional, Type

# core_engine.py 가 같은 디렉토리에 있다고 가정
from core_engine import BaseStrategy, CompositeStrategy


class StrategyLoader:
    """
    strategies/ 디렉토리를 스캔하여 BaseStrategy 서브클래스를 자동 탐색·로드한다.
    새 전략 파일을 추가하면 프로그램 재시작 없이 UI에 반영된다.
    """

    def __init__(self, strategy_dir: str = "strategies"):
        self.strategy_dir = Path(strategy_dir)

    # ── 전략 클래스 목록 반환 (UI 체크박스 렌더링용) ─────────────
    def scan(self) -> Dict[str, Type[BaseStrategy]]:
        """
        반환 예시:
        {
            "RSI":    RsiStrategy,
            "MACD":   MacdStrategy,
            "이격도":  DisparityStrategy,
        }
        """
        result: Dict[str, Type[BaseStrategy]] = {}
        for py_file in sorted(self.strategy_dir.glob("*.py")):
            classes = self._load_classes(py_file)
            for cls in classes:
                result[cls.NAME] = cls
        return result

    # ── 설정 딕셔너리로 CompositeStrategy 조립 ──────────────────
    def build_composite(
        self,
        selected_names: List[str],
        logic: str,
        params: Optional[Dict[str, dict]] = None,
    ) -> CompositeStrategy:
        """
        UI 설정값을 받아 CompositeStrategy 인스턴스를 반환한다.

        Args:
            selected_names : 사용자가 선택한 전략 이름 목록  ["RSI", "MACD"]
            logic          : 신호 조합 방식  "AND" | "OR" | "VOTE"
            params         : 전략별 파라미터  {"RSI": {"period": 14}, ...}

        Returns:
            CompositeStrategy 인스턴스
        """
        available = self.scan()
        params = params or {}
        strategies = []
        for name in selected_names:
            cls = available.get(name)
            if cls is None:
                raise ValueError(f"전략 '{name}'을 찾을 수 없습니다.")
            strategies.append(cls(params=params.get(name, {})))
        return CompositeStrategy(strategies, logic=logic)

    # ── 내부: 파일에서 BaseStrategy 서브클래스 추출 ─────────────
    def _load_classes(self, path: Path) -> List[Type[BaseStrategy]]:
        spec = importlib.util.spec_from_file_location(path.stem, path)
        module = importlib.util.module_from_spec(spec)
        try:
            spec.loader.exec_module(module)
        except Exception as e:
            print(f"[StrategyLoader] {path.name} 로드 실패: {e}")
            return []

        classes = []
        for _, obj in inspect.getmembers(module, inspect.isclass):
            if (
                issubclass(obj, BaseStrategy)
                and obj is not BaseStrategy
                and obj is not CompositeStrategy
                and obj.__module__ == module.__name__
            ):
                classes.append(obj)
        return classes


# ── 사용 예시 ─────────────────────────────────────────────────
if __name__ == "__main__":
    loader = StrategyLoader("strategies")

    # 1. 사용 가능한 전략 목록 출력 (UI 체크박스 데이터)
    available = loader.scan()
    print("사용 가능한 전략:", list(available.keys()))

    # 2. UI 설정값으로 CompositeStrategy 조립
    user_config = {
        "BTC": {
            "strategies": ["RSI", "이격도"],
            "logic": "AND",
            "params": {
                "RSI": {"period": 14, "oversold": 30, "overbought": 70},
                "이격도": {"period": 20, "buy_threshold": -5.0},
            },
        },
        "ETH": {
            "strategies": ["MACD"],
            "logic": "OR",
            "params": {"MACD": {"fast": 12, "slow": 26, "signal": 9}},
        },
    }

    for ticker, cfg in user_config.items():
        composite = loader.build_composite(
            selected_names=cfg["strategies"],
            logic=cfg["logic"],
            params=cfg["params"],
        )
        print(f"{ticker}: {[s.NAME for s in composite.strategies]} / {composite.logic}")
