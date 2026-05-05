"use client";

import { cn } from "@/lib/utils";
import { Badge } from "@/components/ui/badge";
import type {
  AgentSignalAPI,
  CalendarEvent,
  CoordinatorSignalAPI,
  TopEvent,
  ZoneExplanation,
} from "@/lib/api";
import { toActionLabel, toConfidenceLabel } from "@/lib/api";

const PAIRS = ["EURUSD", "GBPUSD", "USDJPY", "USDCHF"];
const TF_ORDER = ["M15", "H1", "H4", "D1"];

// ── helpers ──────────────────────────────────────────────────────────────────

function techLabel(v: number | null): string {
  if (v == null) return "—";
  if (v > 0.5) return "BULLISH";
  if (v < -0.5) return "BEARISH";
  if (v > 0.15) return "SL. BULLISH";
  if (v < -0.15) return "SL. BEARISH";
  return "NEUTRAL";
}

function macroLabel(v: string | null): string {
  if (!v) return "—";
  if (v === "up") return "BULLISH";
  if (v === "down") return "BEARISH";
  return v.toUpperCase();
}

function directionBadgeClass(positive: boolean | null): string {
  if (positive === null) return "bg-muted text-muted-foreground border-border";
  return positive
    ? "bg-emerald-500/10 text-emerald-600 border-emerald-500/25"
    : "bg-red-500/10 text-red-600 border-red-500/25";
}

function scoreColor(v: number): string {
  if (v > 0.2) return "text-emerald-600";
  if (v < -0.2) return "text-red-500";
  return "text-amber-500";
}

// ── primitives ───────────────────────────────────────────────────────────────

function ScoreBar({ value, max = 1 }: { value: number | null; max?: number }) {
  if (value == null) return <div className="h-1 w-full rounded-full bg-muted" />;
  const pct = Math.min((Math.abs(value) / max) * 50, 50);
  const pos = value >= 0;
  return (
    <div className="relative h-1 w-full overflow-hidden rounded-full bg-muted">
      <div className="absolute inset-y-0 left-1/2 w-px bg-border" />
      <div
        className={cn("absolute inset-y-0 rounded-full", pos ? "bg-emerald-500" : "bg-red-500")}
        style={pos ? { left: "50%", width: `${pct}%` } : { right: "50%", width: `${pct}%` }}
      />
    </div>
  );
}

function ProgressBar({
  value,
  colorClass = "bg-primary",
}: {
  value: number | null;
  colorClass?: string;
}) {
  const pct = value != null ? Math.min(value * 100, 100) : 0;
  return (
    <div className="h-1 w-full overflow-hidden rounded-full bg-muted">
      <div className={cn("h-full rounded-full", colorClass)} style={{ width: `${pct}%` }} />
    </div>
  );
}

function Stat({ label, value, mono }: { label: string; value: string; mono?: boolean }) {
  return (
    <div className="flex items-center gap-1.5">
      <span className="text-[10px] uppercase tracking-wide text-muted-foreground">{label}</span>
      <span className={cn("text-xs font-semibold", mono ? "font-mono" : "")}>{value}</span>
    </div>
  );
}

function Row({
  label,
  children,
}: {
  label: string;
  children: React.ReactNode;
}) {
  return (
    <div className="flex items-center justify-between gap-2 py-0.5 text-xs">
      <span className="shrink-0 text-muted-foreground">{label}</span>
      <span className="text-right">{children}</span>
    </div>
  );
}

function AgentCard({
  title,
  badge,
  badgeClass,
  children,
}: {
  title: string;
  badge?: string;
  badgeClass?: string;
  children: React.ReactNode;
}) {
  return (
    <div className="flex flex-col rounded-xl border border-border bg-card shadow-sm overflow-hidden">
      <div className="flex items-center justify-between gap-2 border-b border-border px-4 py-3 shrink-0">
        <h3 className="text-xs font-semibold uppercase tracking-wider text-muted-foreground">
          {title}
        </h3>
        {badge && (
          <span
            className={cn(
              "rounded-md border px-2 py-0.5 text-[10px] font-bold uppercase tracking-wide",
              badgeClass,
            )}
          >
            {badge}
          </span>
        )}
      </div>
      <div className="flex-1 overflow-auto p-4 space-y-3">{children}</div>
    </div>
  );
}

function ZoneBlock({
  title,
  zone,
}: {
  title: string;
  zone: ZoneExplanation | null | undefined;
}) {
  if (!zone) return null;
  const topFeatures = Object.entries(zone.feature_zscores ?? {})
    .sort((a, b) => Math.abs(b[1]) - Math.abs(a[1]))
    .slice(0, 3);
  return (
    <div className="rounded-md border border-border/60 bg-muted/20 px-3 py-2">
      <div className="flex items-center justify-between mb-1">
        <span className="text-[10px] font-semibold uppercase text-muted-foreground">{title}</span>
        <span className="text-[10px] font-mono bg-muted px-1.5 py-0.5 rounded">{zone.zone}</span>
      </div>
      <p className="text-xs font-medium truncate mb-2">{zone.dominant_driver}</p>
      <div className="space-y-1.5">
        {topFeatures.map(([feat, z]) => (
          <div key={feat} className="flex items-center gap-2">
            <span className="w-24 shrink-0 truncate text-[9px] text-muted-foreground">{feat}</span>
            <ScoreBar value={z} max={3} />
            <span
              className={cn(
                "w-10 shrink-0 text-right text-[9px] font-mono",
                z > 0 ? "text-emerald-500" : "text-red-500",
              )}
            >
              {z > 0 ? "+" : ""}
              {z.toFixed(1)}
            </span>
          </div>
        ))}
      </div>
    </div>
  );
}

// ── agent cards ──────────────────────────────────────────────────────────────

function TechnicalCard({ signal }: { signal: AgentSignalAPI | undefined }) {
  const td = signal?.tech_direction ?? null;
  const label = techLabel(td);
  const isBull = td != null && td > 0.3;
  const isBear = td != null && td < -0.3;
  const barColor = isBull ? "bg-emerald-500" : isBear ? "bg-red-500" : "bg-amber-500";
  const badgeCls = isBull
    ? "bg-emerald-500/10 text-emerald-600 border-emerald-500/25"
    : isBear
      ? "bg-red-500/10 text-red-600 border-red-500/25"
      : "bg-amber-500/10 text-amber-600 border-amber-500/25";

  const sortedVotes =
    signal?.tech_timeframe_votes != null
      ? TF_ORDER.map((tf) => ({
          tf,
          v: (signal.tech_timeframe_votes as Record<string, number>)[tf] ?? null,
        })).filter((x) => x.v != null)
      : [];

  return (
    <AgentCard title="Technical" badge={label} badgeClass={cn("border", badgeCls)}>
      <div>
        <div className="flex items-center justify-between mb-1">
          <span className="text-[10px] uppercase tracking-wide text-muted-foreground">
            Confidence
          </span>
          <span className={cn("text-sm font-bold", isBull ? "text-emerald-600" : isBear ? "text-red-500" : "text-amber-500")}>
            {signal?.tech_confidence != null
              ? `${(signal.tech_confidence * 100).toFixed(0)}%`
              : "—"}
          </span>
        </div>
        <ProgressBar value={signal?.tech_confidence ?? null} colorClass={barColor} />
      </div>

      <Row label="Vol regime">
        <span className="font-medium">{signal?.tech_vol_regime ?? "—"}</span>
      </Row>

      {sortedVotes.length > 0 && (
        <div>
          <p className="text-[10px] uppercase tracking-wide text-muted-foreground mb-2">
            Timeframe Votes
          </p>
          <div className="space-y-1.5">
            {sortedVotes.map(({ tf, v }) => (
              <div key={tf} className="flex items-center gap-2">
                <span className="w-8 shrink-0 text-[10px] font-mono text-muted-foreground">
                  {tf}
                </span>
                <ScoreBar value={v} />
                <span
                  className={cn(
                    "w-12 shrink-0 text-right text-[10px] font-mono",
                    v! > 0 ? "text-emerald-500" : "text-red-500",
                  )}
                >
                  {v! > 0 ? "+" : ""}
                  {v!.toFixed(2)}
                </span>
              </div>
            ))}
          </div>
        </div>
      )}

      {signal?.tech_indicator_snapshot && (
        <div>
          <p className="text-[10px] uppercase tracking-wide text-muted-foreground mb-1.5">
            Indicators
          </p>
          <div className="space-y-0.5">
            <Row label="RSI">
              <span
                className={cn(
                  "font-mono text-xs",
                  signal.tech_indicator_snapshot.rsi > 70
                    ? "text-red-500"
                    : signal.tech_indicator_snapshot.rsi < 30
                      ? "text-emerald-500"
                      : "",
                )}
              >
                {signal.tech_indicator_snapshot.rsi.toFixed(1)}
                {signal.tech_indicator_snapshot.rsi > 70
                  ? " OB"
                  : signal.tech_indicator_snapshot.rsi < 30
                    ? " OS"
                    : ""}
              </span>
            </Row>
            <Row label="MACD hist">
              <span
                className={cn(
                  "font-mono text-xs",
                  signal.tech_indicator_snapshot.macd_hist > 0
                    ? "text-emerald-500"
                    : "text-red-500",
                )}
              >
                {signal.tech_indicator_snapshot.macd_hist.toFixed(5)}
              </span>
            </Row>
            <Row label="BB%">
              <span
                className={cn(
                  "font-mono text-xs",
                  signal.tech_indicator_snapshot.bb_pct > 0.8
                    ? "text-red-500"
                    : signal.tech_indicator_snapshot.bb_pct < 0.2
                      ? "text-emerald-500"
                      : "",
                )}
              >
                {signal.tech_indicator_snapshot.bb_pct.toFixed(3)}
              </span>
            </Row>
            <Row label="Above EMA200">
              <span
                className={cn(
                  "text-xs font-medium",
                  signal.tech_indicator_snapshot.above_ema200 ? "text-emerald-500" : "text-red-500",
                )}
              >
                {signal.tech_indicator_snapshot.above_ema200 ? "YES" : "NO"}
              </span>
            </Row>
            <Row label="ATR rank">
              <span className="font-mono text-xs">
                {signal.tech_indicator_snapshot.atr_pct_rank.toFixed(3)}
              </span>
            </Row>
          </div>
        </div>
      )}
    </AgentCard>
  );
}

function MacroCard({ signal }: { signal: AgentSignalAPI | undefined }) {
  const md = signal?.macro_direction ?? null;
  const label = macroLabel(md);
  const isBull = md === "up";
  const isBear = md === "down";
  const badgeCls = isBull
    ? "bg-emerald-500/10 text-emerald-600 border-emerald-500/25"
    : isBear
      ? "bg-red-500/10 text-red-600 border-red-500/25"
      : "bg-amber-500/10 text-amber-600 border-amber-500/25";
  const barColor = isBull ? "bg-emerald-500" : isBear ? "bg-red-500" : "bg-amber-500";

  const subScores = [
    { key: "carry", label: "Carry", value: signal?.macro_carry_score ?? null },
    { key: "regime", label: "Regime", value: signal?.macro_regime_score ?? null },
    { key: "fundamental", label: "Fundamental", value: signal?.macro_fundamental_score ?? null },
    { key: "surprise", label: "Surprise", value: signal?.macro_surprise_score ?? null },
    { key: "bias", label: "Bias", value: signal?.macro_bias_score ?? null },
  ];

  return (
    <AgentCard title="Macro" badge={label} badgeClass={cn("border", badgeCls)}>
      <div>
        <div className="flex items-center justify-between mb-1">
          <span className="text-[10px] uppercase tracking-wide text-muted-foreground">
            Confidence
          </span>
          <span
            className={cn(
              "text-sm font-bold",
              isBull ? "text-emerald-600" : isBear ? "text-red-500" : "text-amber-500",
            )}
          >
            {signal?.macro_confidence != null
              ? `${(signal.macro_confidence * 100).toFixed(0)}%`
              : "—"}
          </span>
        </div>
        <ProgressBar value={signal?.macro_confidence ?? null} colorClass={barColor} />
      </div>

      {signal?.macro_dominant_driver && (
        <div className="rounded-md bg-muted/40 px-3 py-2">
          <p className="text-[10px] uppercase tracking-wide text-muted-foreground">
            Dominant Driver
          </p>
          <p className="mt-1 truncate text-xs font-semibold">{signal.macro_dominant_driver}</p>
        </div>
      )}

      <div>
        <p className="text-[10px] uppercase tracking-wide text-muted-foreground mb-2">
          Sub-Scores
        </p>
        <div className="space-y-2">
          {subScores.map(({ key, label, value }) => (
            <div key={key}>
              <div className="flex items-center justify-between mb-0.5">
                <span className="text-[10px] text-muted-foreground">{label}</span>
                <span
                  className={cn(
                    "text-[10px] font-mono",
                    value != null ? scoreColor(value) : "text-muted-foreground",
                  )}
                >
                  {value != null ? (value > 0 ? `+${value.toFixed(2)}` : value.toFixed(2)) : "—"}
                </span>
              </div>
              <ScoreBar value={value} />
            </div>
          ))}
        </div>
      </div>

      {signal?.macro_top_calendar_events && signal.macro_top_calendar_events.length > 0 && (
        <div>
          <p className="text-[10px] uppercase tracking-wide text-muted-foreground mb-1.5">
            Calendar Events
          </p>
          <div className="space-y-1">
            {signal.macro_top_calendar_events.slice(0, 5).map((ev: CalendarEvent, i: number) => (
              <div
                key={i}
                className="flex items-center justify-between gap-2 rounded-md bg-muted/30 px-2 py-1.5"
              >
                <div className="min-w-0">
                  <p className="truncate text-[10px] font-medium">{ev.event_name}</p>
                  <p className="text-[9px] text-muted-foreground">
                    {ev.country} · weight {ev.impact_weight.toFixed(1)}
                  </p>
                </div>
                <span
                  className={cn(
                    "shrink-0 text-[10px] font-mono font-semibold",
                    ev.contribution >= 0 ? "text-emerald-500" : "text-red-500",
                  )}
                >
                  {ev.contribution >= 0 ? "+" : ""}
                  {ev.contribution.toFixed(3)}
                </span>
              </div>
            ))}
          </div>
        </div>
      )}
    </AgentCard>
  );
}

function GeoCard({ signal }: { signal: AgentSignalAPI | undefined }) {
  const regime = signal?.geo_risk_regime ?? null;
  const bilateral = signal?.geo_bilateral_risk ?? null;
  const badgeCls =
    regime === "low"
      ? "bg-emerald-500/10 text-emerald-600 border-emerald-500/25"
      : regime === "high"
        ? "bg-red-500/10 text-red-600 border-red-500/25"
        : "bg-amber-500/10 text-amber-600 border-amber-500/25";
  const barColor =
    bilateral != null && bilateral > 0.6
      ? "bg-red-500"
      : bilateral != null && bilateral > 0.35
        ? "bg-amber-500"
        : "bg-emerald-500";

  return (
    <AgentCard
      title="Geopolitical"
      badge={regime?.toUpperCase() ?? "—"}
      badgeClass={cn("border", badgeCls)}
    >
      <div>
        <div className="flex items-center justify-between mb-1">
          <span className="text-[10px] uppercase tracking-wide text-muted-foreground">
            Bilateral Risk
          </span>
          <span className="text-sm font-bold font-mono">
            {bilateral != null ? bilateral.toFixed(3) : "—"}
          </span>
        </div>
        <ProgressBar value={bilateral} colorClass={barColor} />
      </div>

      <ZoneBlock title="Base Currency" zone={signal?.geo_base_zone_explanation} />
      <ZoneBlock title="Quote Currency" zone={signal?.geo_quote_zone_explanation} />

      {signal?.geo_top_events && signal.geo_top_events.length > 0 && (
        <div>
          <p className="text-[10px] uppercase tracking-wide text-muted-foreground mb-1.5">
            GDELT Events
          </p>
          <div className="space-y-1.5">
            {signal.geo_top_events.slice(0, 3).map((ev: TopEvent, i: number) => (
              <div key={i} className="rounded-md bg-muted/30 px-2 py-1.5 text-[10px]">
                <div className="flex items-center justify-between gap-1">
                  <span className="truncate font-medium">
                    {ev.actor1_name ?? "?"} → {ev.actor2_name ?? "?"}
                  </span>
                  <span
                    className={cn(
                      "shrink-0 font-mono font-semibold",
                      ev.goldstein_scale >= 0 ? "text-emerald-500" : "text-red-500",
                    )}
                  >
                    {ev.goldstein_scale >= 0 ? "+" : ""}
                    {ev.goldstein_scale.toFixed(1)}
                  </span>
                </div>
                <p className="mt-0.5 text-[9px] text-muted-foreground">
                  tone {ev.avg_tone.toFixed(1)} · {ev.num_mentions} mentions
                </p>
              </div>
            ))}
          </div>
        </div>
      )}
    </AgentCard>
  );
}

function SentimentCard({ signal }: { signal: AgentSignalAPI | undefined }) {
  const stressed = signal?.composite_stress_flag ?? null;
  const sources = signal?.sentiment_stress_sources ?? [];
  const badgeCls =
    stressed === null
      ? "bg-muted text-muted-foreground border-border"
      : stressed
        ? "bg-red-500/10 text-red-600 border-red-500/25"
        : "bg-emerald-500/10 text-emerald-600 border-emerald-500/25";

  const zScores = [
    { label: "GDELT Tone", value: signal?.gdelt_tone_zscore ?? null },
    { label: "GDELT Attention", value: signal?.gdelt_attention_zscore ?? null },
    { label: "Macro Attention", value: signal?.macro_attention_zscore ?? null },
  ];

  return (
    <AgentCard
      title="Sentiment"
      badge={stressed === null ? "—" : stressed ? "STRESSED" : "NORMAL"}
      badgeClass={cn("border", badgeCls)}
    >
      <div>
        <p className="text-[10px] uppercase tracking-wide text-muted-foreground mb-1.5">
          Stress Sources
        </p>
        {sources.length > 0 ? (
          <div className="flex flex-wrap gap-1">
            {sources.map((s: string, i: number) => (
              <span
                key={i}
                className="rounded-full border border-red-500/20 bg-red-500/10 px-2 py-0.5 text-[9px] font-medium text-red-600"
              >
                {s}
              </span>
            ))}
          </div>
        ) : (
          <p className="text-xs text-muted-foreground">No active stress sources</p>
        )}
      </div>

      <div>
        <p className="text-[10px] uppercase tracking-wide text-muted-foreground mb-2">
          Z-Scores (σ)
        </p>
        <div className="space-y-2">
          {zScores.map(({ label, value }) => (
            <div key={label}>
              <div className="flex items-center justify-between mb-0.5">
                <span className="text-[10px] text-muted-foreground">{label}</span>
                <span
                  className={cn(
                    "text-[10px] font-mono",
                    value != null ? scoreColor(value) : "text-muted-foreground",
                  )}
                >
                  {value != null
                    ? `${value > 0 ? "+" : ""}${value.toFixed(2)}σ`
                    : "—"}
                </span>
              </div>
              <ScoreBar value={value} max={3} />
            </div>
          ))}
        </div>
      </div>

      {signal?.usdjpy_stocktwits_vol_signal != null && (
        <div className="rounded-md border border-border/60 bg-muted/20 px-3 py-2">
          <p className="text-[10px] uppercase tracking-wide text-muted-foreground">
            StockTwits Vol Signal
          </p>
          <div className="mt-1 flex items-center justify-between gap-2">
            <span className="font-mono text-xs font-bold">
              {signal.usdjpy_stocktwits_vol_signal.toFixed(4)}
            </span>
            <span
              className={cn(
                "text-xs font-medium",
                signal.usdjpy_stocktwits_vol_signal > 0 ? "text-emerald-500" : "text-red-500",
              )}
            >
              {signal.usdjpy_stocktwits_vol_signal > 0 ? "↑ Elevated" : "↓ Suppressed"}
            </span>
          </div>
          <div className="mt-1.5">
            <ProgressBar
              value={(signal.usdjpy_stocktwits_vol_signal + 1) / 2}
              colorClass="bg-primary"
            />
          </div>
        </div>
      )}
    </AgentCard>
  );
}

// ── main export ──────────────────────────────────────────────────────────────

export interface DeepDiveContentProps {
  pair: string;
  onPairChange: (p: string) => void;
  coordinatorSignals: Map<string, CoordinatorSignalAPI>;
  agentSignals: Map<string, AgentSignalAPI>;
}

export function DeepDiveContent({
  pair,
  onPairChange,
  coordinatorSignals,
  agentSignals,
}: DeepDiveContentProps) {
  const cs = coordinatorSignals.get(pair);
  const signal = agentSignals.get(pair);
  const action = toActionLabel(cs?.suggested_action ?? null);

  return (
    <div className="flex h-full flex-col overflow-hidden bg-[#f0f2f5] dark:bg-muted/20">
      {/* Pair tabs */}
      <div className="flex shrink-0 items-center gap-1 border-b border-border bg-card/90 px-4 py-2">
        {PAIRS.map((p) => {
          const pcs = coordinatorSignals.get(p);
          const isTopPick = pcs?.is_top_pick === true;
          return (
            <button
              key={p}
              onClick={() => onPairChange(p)}
              className={cn(
                "relative rounded-md px-3 py-1.5 text-xs font-medium transition-colors",
                p === pair
                  ? "bg-primary text-primary-foreground"
                  : "text-muted-foreground hover:bg-muted hover:text-foreground",
              )}
            >
              {p}
              {isTopPick && (
                <span className="absolute -right-0.5 -top-0.5 h-1.5 w-1.5 rounded-full bg-amber-400" />
              )}
            </button>
          );
        })}
        <span className="ml-2 text-[9px] text-muted-foreground">● top pick</span>
      </div>

      {/* Alpha summary strip */}
      <div className="shrink-0 border-b border-border bg-card/70 px-5 py-2.5">
        <div className="flex flex-wrap items-center gap-x-5 gap-y-1.5">
          <div className="flex items-center gap-2">
            <span className="text-[10px] uppercase tracking-wide text-muted-foreground">
              Action
            </span>
            <span
              className={cn(
                "rounded-md px-2 py-0.5 text-[10px] font-bold",
                action === "BUY"
                  ? "bg-emerald-500/15 text-emerald-600"
                  : action === "SELL"
                    ? "bg-red-500/15 text-red-600"
                    : "bg-muted text-muted-foreground",
              )}
            >
              {action}
            </span>
          </div>
          <Stat
            label="Conviction"
            value={
              cs?.conviction_score != null ? `${Math.round(cs.conviction_score * 100)}%` : "—"
            }
          />
          <Stat label="Tier" value={toConfidenceLabel(cs?.confidence_tier ?? null)} />
          <Stat
            label="Pos Size"
            value={cs?.position_size_pct != null ? `${cs.position_size_pct.toFixed(1)}%` : "—"}
            mono
          />
          <Stat
            label="SL"
            value={cs?.sl_pct != null ? `${cs.sl_pct.toFixed(2)}%` : "—"}
            mono
          />
          <Stat
            label="TP"
            value={cs?.tp_pct != null ? `${cs.tp_pct.toFixed(2)}%` : "—"}
            mono
          />
          <Stat
            label="R:R"
            value={cs?.risk_reward_ratio != null ? cs.risk_reward_ratio.toFixed(2) : "—"}
            mono
          />
          <Stat
            label="IC"
            value={cs?.direction_ic != null ? cs.direction_ic.toFixed(3) : "—"}
            mono
          />
          <Stat label="Source" value={cs?.direction_source ?? "—"} />
          <Stat label="Horizon" value={cs?.direction_horizon ?? "—"} />
          {cs?.flat_reason && (
            <span className="text-[10px] text-amber-500">⚠ {cs.flat_reason}</span>
          )}
        </div>
      </div>

      {/* 4-agent grid */}
      <div className="flex-1 overflow-auto p-4">
        <div className="grid grid-cols-2 gap-4 xl:grid-cols-4">
          <TechnicalCard signal={signal} />
          <MacroCard signal={signal} />
          <GeoCard signal={signal} />
          <SentimentCard signal={signal} />
        </div>
      </div>
    </div>
  );
}
