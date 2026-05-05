"use client";

import { useState } from "react";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Tabs, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { cn } from "@/lib/utils";
import {
  AgentSignalAPI,
  CoordinatorReportAPI,
  CoordinatorSignalAPI,
  toActionLabel,
  toConfidenceLabel,
  ActionLabel,
} from "@/lib/api";

const PAIRS = ["EURUSD", "GBPUSD", "USDJPY", "USDCHF"];

function actionClass(action: ActionLabel) {
  if (action === "BUY") return "bg-[var(--buy)] text-white";
  if (action === "SELL") return "bg-[var(--sell)] text-white";
  return "bg-muted text-muted-foreground";
}

function OrderControls({ symbol }: { symbol: string }) {
  const [orderType, setOrderType] = useState<"market" | "pending">("market");
  const [size, setSize] = useState("0.10");
  const [sl, setSl] = useState("");
  const [tp, setTp] = useState("");

  void symbol;

  return (
    <div className="border-t border-border p-3 space-y-3">
      <div className="flex gap-2">
        <Button className="flex-1 bg-[var(--buy)] hover:bg-[var(--buy)]/90 text-white h-9" size="sm">
          <div className="flex flex-col items-center">
            <span className="text-[10px] font-normal">BUY</span>
          </div>
        </Button>
        <Button className="flex-1 bg-[var(--sell)] hover:bg-[var(--sell)]/90 text-white h-9" size="sm">
          <div className="flex flex-col items-center">
            <span className="text-[10px] font-normal">SELL</span>
          </div>
        </Button>
      </div>

      <div className="flex gap-2">
        <Button
          variant={orderType === "market" ? "default" : "outline"}
          size="sm"
          className="flex-1 h-7 text-xs"
          onClick={() => setOrderType("market")}
        >
          Market
        </Button>
        <Button
          variant={orderType === "pending" ? "default" : "outline"}
          size="sm"
          className="flex-1 h-7 text-xs"
          onClick={() => setOrderType("pending")}
        >
          Pending
        </Button>
      </div>

      <div className="space-y-2">
        <div>
          <label className="text-[10px] text-muted-foreground uppercase tracking-wider">Size (lots)</label>
          <Input value={size} onChange={(e) => setSize(e.target.value)} className="h-7 text-xs font-mono mt-1" />
        </div>
        <div className="grid grid-cols-2 gap-2">
          <div>
            <label className="text-[10px] text-muted-foreground uppercase tracking-wider">Stop Loss</label>
            <Input value={sl} onChange={(e) => setSl(e.target.value)} placeholder="Price" className="h-7 text-xs font-mono mt-1" />
          </div>
          <div>
            <label className="text-[10px] text-muted-foreground uppercase tracking-wider">Take Profit</label>
            <Input value={tp} onChange={(e) => setTp(e.target.value)} placeholder="Price" className="h-7 text-xs font-mono mt-1" />
          </div>
        </div>
      </div>

      <Button className="w-full h-8 text-xs">Place Order</Button>
    </div>
  );
}

interface RightPanelProps {
  symbol: string;
  width?: number;
  report: CoordinatorReportAPI | null;
  coordinatorSignals: Map<string, CoordinatorSignalAPI>;
  agentSignals: Map<string, AgentSignalAPI>;
}

export function RightPanel({
  symbol,
  width = 360,
  report,
  coordinatorSignals,
  agentSignals,
}: RightPanelProps) {
  const [activePairTab, setActivePairTab] = useState("EURUSD");

  const topPickPair = report?.top_pick ?? null;
  const topPickCs = topPickPair ? coordinatorSignals.get(topPickPair) : null;
  const topPickAction = toActionLabel(topPickCs?.suggested_action ?? null);
  const topPickConviction =
    topPickCs?.conviction_score != null ? Math.round(topPickCs.conviction_score * 100) : null;
  const topPickTier = toConfidenceLabel(topPickCs?.confidence_tier ?? null);
  const topPickPosSz = topPickCs?.position_size_pct;
  const topPickSl = topPickCs?.sl_pct;
  const topPickTp = topPickCs?.tp_pct;

  const activeCs = coordinatorSignals.get(activePairTab);
  const activeAs = agentSignals.get(activePairTab);
  const activeAction = toActionLabel(activeCs?.suggested_action ?? null);

  const hasData = coordinatorSignals.size > 0;

  // Agent highlights derived from the active pair's signals
  const agentHighlights = [
    {
      agent: "Technical",
      line1: activeAs?.tech_vol_regime
        ? `Vol regime: ${activeAs.tech_vol_regime}`
        : "—",
      line2: activeAs?.tech_timeframe_votes
        ? `Votes: D1=${activeAs.tech_timeframe_votes["D1"] ?? "—"} H4=${activeAs.tech_timeframe_votes["H4"] ?? "—"} H1=${activeAs.tech_timeframe_votes["H1"] ?? "—"}`
        : "Timeframe votes: —",
    },
    {
      agent: "Macro",
      line1: activeAs?.macro_dominant_driver
        ? `Driver: ${activeAs.macro_dominant_driver}`
        : "—",
      line2: activeAs?.macro_bias_score != null
        ? `Bias: ${activeAs.macro_bias_score.toFixed(3)}`
        : "Bias: —",
    },
    {
      agent: "Geo",
      line1: activeAs?.geo_risk_regime
        ? `Risk regime: ${activeAs.geo_risk_regime}`
        : "—",
      line2: activeAs?.geo_base_zone_explanation?.dominant_driver
        ? `Driver: ${activeAs.geo_base_zone_explanation.dominant_driver}`
        : "Driver: —",
    },
    {
      agent: "Sentiment",
      line1: activeAs?.composite_stress_flag
        ? `Stress: ${activeAs.sentiment_stress_sources?.join(", ") || "flagged"}`
        : "Stress flag: Normal",
      line2: activeAs?.gdelt_tone_zscore != null
        ? `GDELT tone z: ${activeAs.gdelt_tone_zscore.toFixed(2)}`
        : "GDELT tone: —",
    },
  ];

  return (
    <aside
      className="bg-card border-l border-border flex flex-col shrink-0 overflow-hidden shadow-[var(--card-shadow)]"
      style={{ width: `${width}px` }}
    >
      <div className="p-3 border-b border-border">
        <h2 className="text-sm font-semibold">Alpha Assistant</h2>
      </div>

      <div className="flex-1 overflow-auto">
        {/* Today's Call */}
        <div className="p-3 border-b border-border">
          <h3 className="text-xs font-semibold uppercase tracking-wider text-muted-foreground mb-3">
            {"Today's Call"}
          </h3>

          <div className="bg-muted/50 rounded-md p-3 space-y-3">
            {hasData && topPickPair ? (
              <>
                <div className="grid grid-cols-3 gap-2 text-center">
                  <div>
                    <p className="text-[10px] text-muted-foreground">Top Pick</p>
                    <p className="font-semibold text-sm">{topPickPair}</p>
                  </div>
                  <div>
                    <p className="text-[10px] text-muted-foreground">Direction</p>
                    <Badge className={cn("text-[10px] px-2 h-5 mt-0.5", actionClass(topPickAction))}>
                      {topPickAction}
                    </Badge>
                  </div>
                  <div>
                    <p className="text-[10px] text-muted-foreground">Conviction</p>
                    <p className="font-semibold text-sm text-primary">
                      {topPickConviction != null ? `${topPickConviction}%` : "—"}
                    </p>
                  </div>
                </div>

                <div className="grid grid-cols-3 gap-2 text-center text-xs">
                  <div>
                    <p className="text-[10px] text-muted-foreground">Conf Tier</p>
                    <p className="font-medium">{topPickTier}</p>
                  </div>
                  <div>
                    <p className="text-[10px] text-muted-foreground">Pos Size</p>
                    <p className="font-mono">{topPickPosSz != null ? `${topPickPosSz.toFixed(1)}%` : "—"}</p>
                  </div>
                  <div>
                    <p className="text-[10px] text-muted-foreground">SL / TP %</p>
                    <p className="font-mono">
                      {topPickSl != null && topPickTp != null
                        ? `${topPickSl.toFixed(2)} / ${topPickTp.toFixed(2)}`
                        : "—"}
                    </p>
                  </div>
                </div>

                {report?.global_regime && (
                  <div className="text-[10px] text-muted-foreground text-center">
                    Global regime: <span className="font-medium text-foreground">{report.global_regime}</span>
                  </div>
                )}
                {report?.hold_reason && (
                  <p className="text-xs text-amber-600 leading-relaxed">{report.hold_reason}</p>
                )}
              </>
            ) : (
              <p className="text-xs text-muted-foreground text-center">
                {hasData ? "No top pick" : "Awaiting inference data…"}
              </p>
            )}
          </div>
        </div>

        {/* Per-pair Tabs */}
        <div className="p-3 border-b border-border">
          <Tabs value={activePairTab} onValueChange={setActivePairTab}>
            <TabsList className="w-full h-7 bg-muted p-0.5">
              {PAIRS.map((pair) => (
                <TabsTrigger
                  key={pair}
                  value={pair}
                  className="flex-1 h-6 text-[10px] data-[state=active]:bg-card"
                >
                  {pair}
                </TabsTrigger>
              ))}
            </TabsList>

            <div className="mt-3 space-y-2 text-xs">
              <div className="flex justify-between">
                <span className="text-muted-foreground">Suggested Action</span>
                <Badge className={cn("text-[10px] px-1.5 h-5", actionClass(activeAction))}>
                  {hasData ? activeAction : "—"}
                </Badge>
              </div>
              <div className="flex justify-between">
                <span className="text-muted-foreground">Conviction</span>
                <span className="font-mono">
                  {activeCs?.conviction_score != null
                    ? `${Math.round(activeCs.conviction_score * 100)}%`
                    : "—"}
                </span>
              </div>
              <div className="flex justify-between">
                <span className="text-muted-foreground">Conf Tier</span>
                <span>{toConfidenceLabel(activeCs?.confidence_tier ?? null)}</span>
              </div>
              <div className="flex justify-between">
                <span className="text-muted-foreground">Position Size</span>
                <span className="font-mono">
                  {activeCs?.position_size_pct != null ? `${activeCs.position_size_pct.toFixed(1)}%` : "—"}
                </span>
              </div>
              <div className="flex justify-between">
                <span className="text-muted-foreground">SL / TP %</span>
                <span className="font-mono">
                  {activeCs?.sl_pct != null && activeCs?.tp_pct != null
                    ? `${activeCs.sl_pct.toFixed(2)} / ${activeCs.tp_pct.toFixed(2)}`
                    : "—"}
                </span>
              </div>
              <div className="flex justify-between">
                <span className="text-muted-foreground">Source</span>
                <span className="text-right max-w-[150px] truncate">{activeCs?.direction_source ?? "—"}</span>
              </div>
              <div className="flex justify-between">
                <span className="text-muted-foreground">Horizon</span>
                <span>{activeCs?.direction_horizon ?? "—"}</span>
              </div>
              <div className="flex justify-between">
                <span className="text-muted-foreground">Est Vol 3d</span>
                <span className="font-mono">
                  {activeCs?.estimated_vol_3d != null
                    ? activeCs.estimated_vol_3d.toFixed(4)
                    : "—"}
                </span>
              </div>
            </div>
          </Tabs>
        </div>

        {/* Agent Highlights */}
        <div className="p-3 border-b border-border">
          <h3 className="text-xs font-semibold uppercase tracking-wider text-muted-foreground mb-3">
            Agent Highlights — {activePairTab}
          </h3>
          <div className="space-y-2">
            {agentHighlights.map((h) => (
              <div key={h.agent} className="text-xs">
                <span className="font-medium text-primary">{h.agent}: </span>
                <span className="text-muted-foreground">{h.line1}</span>
                <br />
                <span className="text-muted-foreground ml-2">{h.line2}</span>
              </div>
            ))}
          </div>

          {/* Macro calendar events if available */}
          {activeAs?.macro_top_calendar_events && activeAs.macro_top_calendar_events.length > 0 && (
            <div className="mt-3">
              <p className="text-[10px] uppercase tracking-wider text-muted-foreground mb-1.5">
                Top Calendar Events
              </p>
              <div className="space-y-1">
                {activeAs.macro_top_calendar_events.slice(0, 3).map((ev, i) => (
                  <div key={i} className="flex justify-between text-[10px]">
                    <span className="text-foreground font-medium truncate max-w-[140px]">{ev.event_name}</span>
                    <span className={cn(
                      "font-mono",
                      ev.surprise_direction > 0 ? "text-[var(--long)]" : "text-[var(--short)]"
                    )}>
                      {ev.surprise_direction > 0 ? "+" : ""}{ev.contribution.toFixed(3)}
                    </span>
                  </div>
                ))}
              </div>
            </div>
          )}

          {/* Geo top events if available */}
          {activeAs?.geo_top_events && activeAs.geo_top_events.length > 0 && (
            <div className="mt-3">
              <p className="text-[10px] uppercase tracking-wider text-muted-foreground mb-1.5">
                Top Geo Events
              </p>
              <div className="space-y-1">
                {activeAs.geo_top_events.slice(0, 2).map((ev, i) => (
                  <div key={i} className="text-[10px] text-muted-foreground">
                    {ev.actor1_name ?? "—"} ↔ {ev.actor2_name ?? "—"} · tone {ev.avg_tone.toFixed(1)}
                  </div>
                ))}
              </div>
            </div>
          )}
        </div>
      </div>

      <OrderControls symbol={symbol} />
    </aside>
  );
}
