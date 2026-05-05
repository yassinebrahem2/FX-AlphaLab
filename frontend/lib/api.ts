const API_BASE = process.env.NEXT_PUBLIC_API_URL ?? "http://localhost:8000";

async function get<T>(path: string): Promise<T> {
  const res = await fetch(`${API_BASE}${path}`, { cache: "no-store" });
  if (!res.ok) throw new Error(`${res.status} ${path}`);
  return res.json() as Promise<T>;
}

// ── API types ─────────────────────────────────────────────────────────────────

export interface CoordinatorReportAPI {
  date: string;
  top_pick: string | null;
  overall_action: string;
  hold_reason: string | null;
  global_regime: string | null;
  narrative_context: Record<string, unknown> | null;
}

export interface IndicatorSnapshot {
  rsi: number;
  macd_hist: number;
  bb_pct: number;
  above_ema200: boolean;
  atr_pct_rank: number;
}

export interface TopEvent {
  actor1_name: string | null;
  actor2_name: string | null;
  goldstein_scale: number;
  avg_tone: number;
  num_mentions: number;
  quad_class: number;
  source_url: string | null;
}

export interface ZoneExplanation {
  zone: string;
  risk_score: number;
  feature_zscores: Record<string, number>;
  dominant_driver: string;
}

export interface CalendarEvent {
  event_name: string;
  country: string;
  surprise_direction: number;
  surprise_magnitude: number;
  impact_weight: number;
  prob: number;
  contribution: number;
}

export interface AgentSignalAPI {
  date: string;
  pair: string;
  tech_direction: number | null;
  tech_confidence: number | null;
  tech_vol_regime: string | null;
  geo_bilateral_risk: number | null;
  geo_risk_regime: string | null;
  macro_direction: string | null;
  macro_confidence: number | null;
  macro_carry_score: number | null;
  macro_regime_score: number | null;
  macro_fundamental_score: number | null;
  macro_surprise_score: number | null;
  macro_bias_score: number | null;
  macro_dominant_driver: string | null;
  usdjpy_stocktwits_vol_signal: number | null;
  gdelt_tone_zscore: number | null;
  gdelt_attention_zscore: number | null;
  macro_attention_zscore: number | null;
  composite_stress_flag: boolean | null;
  tech_indicator_snapshot: IndicatorSnapshot | null;
  tech_timeframe_votes: Record<string, number> | null;
  geo_top_events: TopEvent[] | null;
  geo_base_zone_explanation: ZoneExplanation | null;
  geo_quote_zone_explanation: ZoneExplanation | null;
  geo_graph: { zone_risk_scores: Record<string, number>; edge_weights: Record<string, number> } | null;
  macro_top_calendar_events: CalendarEvent[] | null;
  sentiment_stress_sources: string[] | null;
  sentiment_stocktwits_breakdown: Record<string, unknown> | null;
}

export interface CoordinatorSignalAPI {
  date: string;
  pair: string;
  vol_signal: number | null;
  vol_source: string | null;
  direction: number | null;
  direction_source: string | null;
  direction_horizon: string | null;
  direction_ic: number | null;
  confidence_tier: string | null;
  flat_reason: string | null;
  regime: string | null;
  suggested_action: string | null;
  conviction_score: number | null;
  position_size_pct: number | null;
  sl_pct: number | null;
  tp_pct: number | null;
  risk_reward_ratio: number | null;
  estimated_vol_3d: number | null;
  is_top_pick: boolean | null;
  overall_action: string | null;
}

export interface DateSignalsAPI {
  date: string;
  agent_signals: AgentSignalAPI[];
  coordinator_signals: CoordinatorSignalAPI[];
}

export interface OHLCVBarAPI {
  timestamp_utc: string;
  open: number;
  high: number;
  low: number;
  close: number;
  volume: number;
}

// ── Fetch functions ───────────────────────────────────────────────────────────

export const fetchLatestReport = () => get<CoordinatorReportAPI>("/reports/latest");

export const fetchSignals = (date: string) => get<DateSignalsAPI>(`/signals/${date}`);

export const fetchOHLCV = (instrument: string, tf = "H1", days = 30) =>
  get<OHLCVBarAPI[]>(`/ohlcv/${instrument}?tf=${tf}&days=${days}`);

// ── Helpers ───────────────────────────────────────────────────────────────────

export type ActionLabel = "BUY" | "SELL" | "HOLD";

export function toActionLabel(suggested_action: string | null): ActionLabel {
  if (suggested_action === "LONG") return "BUY";
  if (suggested_action === "SHORT") return "SELL";
  return "HOLD";
}

export function toConfidenceLabel(tier: string | null): string {
  if (tier === "high") return "3/3";
  if (tier === "medium") return "2/3";
  if (tier === "low") return "1/3";
  return "—";
}
