// Agent output types matching the pipeline specification
// These represent DETERMINISTIC outputs from each agent - no fabricated text

export interface CoordinatorReport {
  // Alpha Generator outputs
  overall_action: "BUY" | "SELL" | "HOLD";
  top_pick: string;
  per_pair: {
    symbol: string;
    suggested_action: "BUY" | "SELL" | "HOLD";
    conviction_score: number;
    confidence_tier: "High" | "Medium" | "Low";
    position_size_pct: number;
    sl_pct: number;
    tp_pct: number;
    direction_source: string;
    direction_horizon: string;
    estimated_vol_3d: number;
    vol_source: string;
    regime: "Trending" | "Ranging" | "Volatile" | "Calm";
    hold_reason: string | null;
  }[];
  stale_inputs: string[];
  // LLM-generated narrative (clearly marked)
  narrative_context: string;
}

export interface TechnicalAgentOutput {
  symbol: string;
  direction: "LONG" | "SHORT" | "FLAT";
  confidence: number;
  volatility_regime: "High" | "Normal" | "Low";
  indicator_snapshot: {
    rsi_14: number;
    macd_histogram: number;
    bb_percent: number;
    ema_200_position: "Above" | "Below";
    atr_14: number;
    atr_rank: "High" | "Normal" | "Low";
  };
  timeframe_votes: {
    timeframe: string;
    direction: "LONG" | "SHORT" | "FLAT";
    weight: number;
  }[];
}

export interface MacroAgentOutput {
  symbol: string;
  module_c_direction: "LONG" | "SHORT" | "NEUTRAL";
  macro_confidence: number;
  macro_bias_score: number;
  dominant_driver: string;
  macro_surprise_score: number;
  top_calendar_events: {
    event: string;
    date: string;
    impact: "High" | "Medium" | "Low";
    forecast: string;
    previous: string;
  }[];
}

export interface GeopoliticalAgentOutput {
  symbol: string;
  bilateral_risk_score: number;
  risk_regime: "Elevated" | "Normal" | "Low";
  base_dominant_driver: string;
  quote_dominant_driver: string;
  top_events: {
    headline: string;
    relevance_score: number;
    sentiment: "Positive" | "Negative" | "Neutral";
  }[];
}

export interface SentimentAgentOutput {
  symbol: string;
  usdjpy_stocktwits_vol_signal: "High" | "Normal" | "Low";
  usdjpy_stocktwits_active: boolean;
  gdelt_tone_zscore: number;
  gdelt_attention_zscore: number;
  macro_attention_zscore: number;
  composite_stress_flag: boolean;
  stress_sources: string[];
}

export interface FullAgentReport {
  timestamp: string;
  coordinator: CoordinatorReport;
  technical: TechnicalAgentOutput[];
  macro: MacroAgentOutput[];
  geopolitical: GeopoliticalAgentOutput[];
  sentiment: SentimentAgentOutput[];
}

// Mock data representing real agent outputs
export const mockAgentReport: FullAgentReport = {
  timestamp: new Date().toISOString(),
  coordinator: {
    overall_action: "BUY",
    top_pick: "EURUSD",
    per_pair: [
      {
        symbol: "EURUSD",
        suggested_action: "BUY",
        conviction_score: 78,
        confidence_tier: "High",
        position_size_pct: 2.5,
        sl_pct: 0.57,
        tp_pct: 1.29,
        direction_source: "Technical + Macro",
        direction_horizon: "1-3 days",
        estimated_vol_3d: 0.0045,
        vol_source: "ATR + GARCH",
        regime: "Trending",
        hold_reason: null,
      },
      {
        symbol: "GBPUSD",
        suggested_action: "SELL",
        conviction_score: 65,
        confidence_tier: "Medium",
        position_size_pct: 1.5,
        sl_pct: 0.54,
        tp_pct: 0.79,
        direction_source: "Sentiment",
        direction_horizon: "Intraday",
        estimated_vol_3d: 0.0052,
        vol_source: "ATR + GARCH",
        regime: "Ranging",
        hold_reason: null,
      },
      {
        symbol: "USDJPY",
        suggested_action: "BUY",
        conviction_score: 72,
        confidence_tier: "High",
        position_size_pct: 2.0,
        sl_pct: 0.40,
        tp_pct: 1.17,
        direction_source: "Macro",
        direction_horizon: "1-2 weeks",
        estimated_vol_3d: 0.0068,
        vol_source: "ATR + GARCH",
        regime: "Trending",
        hold_reason: null,
      },
      {
        symbol: "AUDUSD",
        suggested_action: "SELL",
        conviction_score: 80,
        confidence_tier: "High",
        position_size_pct: 3.0,
        sl_pct: 1.04,
        tp_pct: 1.41,
        direction_source: "Technical + Geo",
        direction_horizon: "2-5 days",
        estimated_vol_3d: 0.0061,
        vol_source: "ATR + GARCH",
        regime: "Volatile",
        hold_reason: null,
      },
    ],
    stale_inputs: ["GDELT feed delayed 4h", "StockTwits GBPUSD inactive"],
    narrative_context:
      "EURUSD presents the strongest opportunity today with technical alignment across multiple timeframes. The 4H and Daily charts show bullish structure with price holding above the 50 EMA. Macro drivers favor EUR strength as the Fed turns dovish while ECB maintains hawkish stance. Retail sentiment is net short, providing contrarian support. Geopolitical risks remain contained with EU-US trade relations stable.",
  },
  technical: [
    {
      symbol: "EURUSD",
      direction: "LONG",
      confidence: 82,
      volatility_regime: "Normal",
      indicator_snapshot: {
        rsi_14: 58.3,
        macd_histogram: 0.00023,
        bb_percent: 67.2,
        ema_200_position: "Above",
        atr_14: 0.00042,
        atr_rank: "Normal",
      },
      timeframe_votes: [
        { timeframe: "1H", direction: "FLAT", weight: 0.2 },
        { timeframe: "4H", direction: "LONG", weight: 0.35 },
        { timeframe: "Daily", direction: "LONG", weight: 0.45 },
      ],
    },
    {
      symbol: "GBPUSD",
      direction: "SHORT",
      confidence: 61,
      volatility_regime: "High",
      indicator_snapshot: {
        rsi_14: 42.1,
        macd_histogram: -0.00018,
        bb_percent: 28.5,
        ema_200_position: "Below",
        atr_14: 0.00058,
        atr_rank: "High",
      },
      timeframe_votes: [
        { timeframe: "1H", direction: "SHORT", weight: 0.2 },
        { timeframe: "4H", direction: "SHORT", weight: 0.35 },
        { timeframe: "Daily", direction: "FLAT", weight: 0.45 },
      ],
    },
    {
      symbol: "USDJPY",
      direction: "LONG",
      confidence: 74,
      volatility_regime: "Normal",
      indicator_snapshot: {
        rsi_14: 62.7,
        macd_histogram: 0.015,
        bb_percent: 78.4,
        ema_200_position: "Above",
        atr_14: 0.52,
        atr_rank: "Normal",
      },
      timeframe_votes: [
        { timeframe: "1H", direction: "LONG", weight: 0.2 },
        { timeframe: "4H", direction: "LONG", weight: 0.35 },
        { timeframe: "Daily", direction: "LONG", weight: 0.45 },
      ],
    },
    {
      symbol: "AUDUSD",
      direction: "SHORT",
      confidence: 79,
      volatility_regime: "High",
      indicator_snapshot: {
        rsi_14: 35.2,
        macd_histogram: -0.00031,
        bb_percent: 12.8,
        ema_200_position: "Below",
        atr_14: 0.00067,
        atr_rank: "High",
      },
      timeframe_votes: [
        { timeframe: "1H", direction: "SHORT", weight: 0.2 },
        { timeframe: "4H", direction: "SHORT", weight: 0.35 },
        { timeframe: "Daily", direction: "SHORT", weight: 0.45 },
      ],
    },
  ],
  macro: [
    {
      symbol: "EURUSD",
      module_c_direction: "LONG",
      macro_confidence: 71,
      macro_bias_score: 0.62,
      dominant_driver: "Fed/ECB divergence",
      macro_surprise_score: 0.34,
      top_calendar_events: [
        { event: "US NFP", date: "Fri 08:30 ET", impact: "High", forecast: "+180K", previous: "+256K" },
        { event: "ECB Lagarde Speech", date: "Thu 14:00 ET", impact: "Medium", forecast: "-", previous: "-" },
        { event: "US ISM Services", date: "Wed 10:00 ET", impact: "Medium", forecast: "52.1", previous: "52.8" },
      ],
    },
    {
      symbol: "GBPUSD",
      module_c_direction: "NEUTRAL",
      macro_confidence: 48,
      macro_bias_score: -0.12,
      dominant_driver: "UK data mixed",
      macro_surprise_score: -0.08,
      top_calendar_events: [
        { event: "BoE Rate Decision", date: "Thu 07:00 ET", impact: "High", forecast: "Hold 5.25%", previous: "5.25%" },
        { event: "UK GDP", date: "Fri 02:00 ET", impact: "Medium", forecast: "0.2%", previous: "0.1%" },
      ],
    },
    {
      symbol: "USDJPY",
      module_c_direction: "LONG",
      macro_confidence: 68,
      macro_bias_score: 0.55,
      dominant_driver: "Carry trade",
      macro_surprise_score: 0.22,
      top_calendar_events: [
        { event: "BoJ Policy Meeting", date: "Tue 23:00 ET", impact: "High", forecast: "Hold", previous: "-0.1%" },
        { event: "Japan CPI", date: "Thu 19:30 ET", impact: "Medium", forecast: "2.8%", previous: "2.9%" },
      ],
    },
    {
      symbol: "AUDUSD",
      module_c_direction: "SHORT",
      macro_confidence: 75,
      macro_bias_score: -0.71,
      dominant_driver: "China slowdown",
      macro_surprise_score: -0.45,
      top_calendar_events: [
        { event: "RBA Minutes", date: "Tue 21:30 ET", impact: "Medium", forecast: "-", previous: "-" },
        { event: "China PMI", date: "Sun 21:00 ET", impact: "High", forecast: "49.5", previous: "49.8" },
      ],
    },
  ],
  geopolitical: [
    {
      symbol: "EURUSD",
      bilateral_risk_score: 0.23,
      risk_regime: "Low",
      base_dominant_driver: "ECB policy normalization",
      quote_dominant_driver: "US election uncertainty",
      top_events: [
        { headline: "EU-US trade talks progress on tariff reduction", relevance_score: 0.82, sentiment: "Positive" },
        { headline: "France budget concerns ease after compromise", relevance_score: 0.65, sentiment: "Positive" },
      ],
    },
    {
      symbol: "GBPUSD",
      bilateral_risk_score: 0.31,
      risk_regime: "Normal",
      base_dominant_driver: "UK fiscal policy",
      quote_dominant_driver: "US election uncertainty",
      top_events: [
        { headline: "UK-EU post-Brexit negotiations continue", relevance_score: 0.71, sentiment: "Neutral" },
        { headline: "UK autumn budget implications for GBP", relevance_score: 0.68, sentiment: "Negative" },
      ],
    },
    {
      symbol: "USDJPY",
      bilateral_risk_score: 0.18,
      risk_regime: "Low",
      base_dominant_driver: "US election uncertainty",
      quote_dominant_driver: "BoJ intervention risk",
      top_events: [
        { headline: "BoJ maintains verbal intervention stance at 155", relevance_score: 0.88, sentiment: "Negative" },
        { headline: "US-Japan security alliance reaffirmed", relevance_score: 0.52, sentiment: "Positive" },
      ],
    },
    {
      symbol: "AUDUSD",
      bilateral_risk_score: 0.67,
      risk_regime: "Elevated",
      base_dominant_driver: "China-Australia relations",
      quote_dominant_driver: "US election uncertainty",
      top_events: [
        { headline: "China property sector concerns intensify", relevance_score: 0.91, sentiment: "Negative" },
        { headline: "Australia iron ore exports face headwinds", relevance_score: 0.78, sentiment: "Negative" },
        { headline: "US-China tariff escalation risks", relevance_score: 0.85, sentiment: "Negative" },
      ],
    },
  ],
  sentiment: [
    {
      symbol: "EURUSD",
      usdjpy_stocktwits_vol_signal: "Normal",
      usdjpy_stocktwits_active: true,
      gdelt_tone_zscore: 0.42,
      gdelt_attention_zscore: 0.18,
      macro_attention_zscore: 0.55,
      composite_stress_flag: false,
      stress_sources: [],
    },
    {
      symbol: "GBPUSD",
      usdjpy_stocktwits_vol_signal: "Low",
      usdjpy_stocktwits_active: false,
      gdelt_tone_zscore: -0.31,
      gdelt_attention_zscore: 0.62,
      macro_attention_zscore: 0.78,
      composite_stress_flag: false,
      stress_sources: [],
    },
    {
      symbol: "USDJPY",
      usdjpy_stocktwits_vol_signal: "High",
      usdjpy_stocktwits_active: true,
      gdelt_tone_zscore: 0.15,
      gdelt_attention_zscore: 0.88,
      macro_attention_zscore: 0.92,
      composite_stress_flag: true,
      stress_sources: ["BoJ intervention speculation", "Yen carry unwind risk"],
    },
    {
      symbol: "AUDUSD",
      usdjpy_stocktwits_vol_signal: "High",
      usdjpy_stocktwits_active: true,
      gdelt_tone_zscore: -0.67,
      gdelt_attention_zscore: 0.71,
      macro_attention_zscore: 0.65,
      composite_stress_flag: true,
      stress_sources: ["China economic data", "Commodity selloff"],
    },
  ],
};
