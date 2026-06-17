import numpy as np

class MarketRegimeDetector:
    """
    일봉의 장기 선형 회귀분석(Linear Regression)과 최근 기간별 추세 가속도(기울기 변화율)를 
    복합 분석하여 현재 시장 국면을 진단하는 클래스.
    
    - BULL (상승장): 장기 회귀 적정가 대비 안정적 지지 및 중단기 추세 우상향인 경우
    - BEAR (하락장): 장기 회귀선 아래에 있으며, 중단기 하락 추세가 지속되는 경우
    - RANGE (횡보장): 장기 극단 과매도 상태에서 하락이 진정/횡보 전환되거나, 회귀선 부근에서 가격이 수렴하는 경우
    """

    def __init__(self, candle_manager, long_term_ma_period=1500, bull_duration_limit_days=100, bear_duration_limit_days=200):
        self.candle_manager = candle_manager
        # long_term_ma_period 변수는 1500일 장기 회귀 분석 기간으로 사용됨
        self.long_term_ma_period = long_term_ma_period
        self.bull_duration_limit_days = bull_duration_limit_days
        self.bear_duration_limit_days = bear_duration_limit_days

    def detect_regime(self) -> str:
        """
        현재 시장 국면을 진단하여 'BULL', 'BEAR', 'RANGE' 중 하나를 반환합니다.
        """
        detailed = self.detect_regime_detailed()
        return detailed["regime"]

    def detect_regime_detailed(self) -> dict:
        """
        1500일 장기 선형 회귀 및 최근 다중 타임프레임(365일, 180일, 90일, 30일) 기울기를 연산하여
        상세한 장세 판정 근거와 보조 지표를 반환합니다.
        """
        # 일봉 종가 리스트 가져오기
        prices = self.candle_manager.get_prices("D")
        n = len(prices)
        
        # 데이터가 분석 기간보다 부족할 경우 가용한 최대 개수를 사용하도록 유연화
        actual_period = self.long_term_ma_period
        if n < actual_period:
            actual_period = n
            
        # 데이터가 최소 분석 기간 미만이면 분석 대기
        min_required = min(300, self.long_term_ma_period)
        if actual_period < min_required:
            return {
                "regime": "RANGE",
                "reason": f"일봉 데이터 부족 ({n}/{min_required}). 분석 대기 중.",
                "metrics": {
                    "days_since_peak": 0,
                    "deviation_pct": 0.0,
                    "slope_pct": 0.0,
                    "slope_rate_365": 0.0,
                    "slope_rate_90": 0.0,
                    "slope_rate_30": 0.0,
                    "fair_value": 0.0
                }
            }

        # 1. 장기 선형 회귀 (Linear Regression)
        p_long = prices[-actual_period:]
        x_long = np.arange(actual_period)
        slope_long, intercept_long = np.polyfit(x_long, p_long, 1)
        
        # 장기 회귀 적정가 및 현재 괴리율 계산
        fair_value = slope_long * (actual_period - 1) + intercept_long
        price_now = float(prices[-1])
        dev_pct = (price_now - fair_value) / fair_value * 100

        # 2. 최근 다중 기간 선형 회귀 기울기 계산 및 현재가 대비 백분율 환산 (Slope Rate % / 일)
        p_365 = prices[-365:]
        slope_365, _ = np.polyfit(np.arange(365), p_365, 1)
        slope_rate_365 = (slope_365 / price_now) * 100
        
        p_180 = prices[-180:]
        slope_180, _ = np.polyfit(np.arange(180), p_180, 1)
        slope_rate_180 = (slope_180 / price_now) * 100
        
        p_90 = prices[-90:]
        slope_90, _ = np.polyfit(np.arange(90), p_90, 1)
        slope_rate_90 = (slope_90 / price_now) * 100
        
        p_30 = prices[-30:]
        slope_30, _ = np.polyfit(np.arange(30), p_30, 1)
        slope_rate_30 = (slope_30 / price_now) * 100

        # 3. 최고점(Peak) 경과 일수 계산 (보조용)
        lookback_period = min(n, 730)
        recent_prices = prices[-lookback_period:]
        peak_price = max(recent_prices)
        peak_index = recent_prices.index(peak_price)
        days_since_peak = lookback_period - 1 - peak_index

        # 4. 종합 조건 판정 로직
        final_regime = "RANGE"
        reason = ""

        # 하락 가속도 둔화(바닥 형성) 여부 판단 필터
        is_bottoming = (slope_rate_90 >= -0.05) or (slope_rate_30 >= 0.0)

        # A. 극단적 저평가 (괴리율 -25% 이하 영역, 강한 장기 매집대)
        if dev_pct <= -25.0:
            if slope_rate_30 >= 0.1:
                final_regime = "BULL"
                reason = f"대세 회귀선 괴리율 극소화({dev_pct:.1f}%) 상태에서 최근 30일 단기 상승 반전 모멘텀(+{slope_rate_30:.3f}%/일) 확인으로 BULL 전환."
            elif is_bottoming:
                final_regime = "RANGE"
                reason = f"대세 회귀선 괴리율 극소화({dev_pct:.1f}%) 상태에서 최근 90일 및 30일 하락 둔화/횡보 신호(90일 기울기율: {slope_rate_90:.3f}%/일) 확인으로 RANGE 전환."
            else:
                final_regime = "BEAR"
                reason = f"대세 회귀선 괴리율 극소화({dev_pct:.1f}%) 상태이나 여전히 단기/중기 하락 추세(30일 기울기율: {slope_rate_30:.3f}%/일)가 우세하여 BEAR 유지."
        
        # B. 대세 상승 영역 (괴리율 -10% 이상이면서 단중기 추세가 양수일 때)
        elif dev_pct >= -10.0 and slope_rate_90 > 0 and slope_rate_30 > 0:
            final_regime = "BULL"
            reason = f"대세 회귀선 괴리 안정화({dev_pct:.1f}%) 및 중단기(90일, 30일) 추세 동시 우상향(BULL) 안착."
            
        # C. 대세 하락 진행 영역 (적정가 하회 및 단중기 추세 하향)
        elif dev_pct < -10.0 and (slope_rate_90 < -0.05 or slope_rate_30 < -0.1):
            final_regime = "BEAR"
            reason = f"장기 회귀 적정가 하회 상태에서 중단기 하락 추세(최근 30일 기울기율: {slope_rate_30:.3f}%/일)가 뚜렷하여 BEAR 판정."
            
        # D. 기본 횡보/수렴 영역
        else:
            final_regime = "RANGE"
            reason = f"대세 괴리율({dev_pct:.1f}%) 수렴 및 최근 90일 기울기율({slope_rate_90:.3f}%/일) 완화로 횡보(RANGE) 국면 판정."

        # 서버 콘솔 출력 (디버깅용)
        print(f"[RegimeDetector] {self.candle_manager.ticker} 상세 | 현재가: {price_now:,.0f} | Fair_{self.long_term_ma_period}: {fair_value:,.0f} | 괴리: {dev_pct:+.2f}% | 30일기울기율: {slope_rate_30:+.3f}% | 최종판정: {final_regime} ({reason})")

        return {
            "regime": final_regime,
            "reason": reason,
            "metrics": {
                "days_since_peak": int(days_since_peak),
                "deviation_pct": float(dev_pct),
                "slope_pct": float(slope_rate_90),  # UI 연동 호환성을 위해 90일 기울기율 매핑
                "slope_rate_365": float(slope_rate_365),
                "slope_rate_180": float(slope_rate_180),
                "slope_rate_90": float(slope_rate_90),
                "slope_rate_30": float(slope_rate_30),
                "fair_value": float(fair_value)
            }
        }


if __name__ == "__main__":
    # 단위 테스트 검증
    class DummyCandleManager:
        def __init__(self, prices, ticker="BTC"):
            self.prices_list = prices
            self.ticker = ticker
        def get_prices(self, tf):
            return self.prices_list

    # 1500일 회귀 테스트용 데이터 생성
    # 1500일 동안 매일 1000씩 꾸준히 상승하는 시나리오
    bull_prices = [50000.0 + i * 1000.0 for i in range(1600)]
    detector = MarketRegimeDetector(DummyCandleManager(bull_prices), long_term_ma_period=1500)
    print("우상향 시나리오 진단 결과 (BULL 예상):", detector.detect_regime())
