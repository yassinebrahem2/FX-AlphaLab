"use client";

import { Search, ChevronDown, ChevronLeft, ChevronRight } from "lucide-react";
import { Input } from "@/components/ui/input";
import { Badge } from "@/components/ui/badge";
import { cn } from "@/lib/utils";
import {
  AgentSignalAPI,
  CoordinatorSignalAPI,
  toActionLabel,
  toConfidenceLabel,
} from "@/lib/api";

const WATCHLIST_PAIRS = ["EURUSD", "GBPUSD", "USDJPY", "USDCHF"];

interface AgentPulseDef {
  name: string;
  driver: (sig: AgentSignalAPI | undefined) => string;
  impact: string;
}

const AGENT_PULSE_DEFS: AgentPulseDef[] = [
  {
    name: "Technical",
    driver: (s) => s?.tech_vol_regime ?? "—",
    impact: "entry (1d)",
  },
  {
    name: "Macro",
    driver: (s) => s?.macro_dominant_driver ?? "—",
    impact: "direction (5d)",
  },
  {
    name: "Geopolitical",
    driver: (s) =>
      s?.geo_base_zone_explanation?.dominant_driver ?? s?.geo_risk_regime ?? "—",
    impact: "volatility (2w)",
  },
  {
    name: "Sentiment",
    driver: (s) => {
      if (!s) return "—";
      if (s.composite_stress_flag) {
        const src = s.sentiment_stress_sources?.[0];
        return src ?? "stress flagged";
      }
      return "normal";
    },
    impact: "regime overlay",
  },
];

interface LeftSidebarProps {
  collapsed: boolean;
  onToggle: () => void;
  activeInstrument: string;
  onInstrumentChange: (symbol: string) => void;
  width?: number;
  coordinatorSignals: Map<string, CoordinatorSignalAPI>;
  agentSignals: Map<string, AgentSignalAPI>;
}

export function LeftSidebar({
  collapsed,
  onToggle,
  activeInstrument,
  onInstrumentChange,
  width = 260,
  coordinatorSignals,
  agentSignals,
}: LeftSidebarProps) {
  if (collapsed) {
    return (
      <aside className="w-10 bg-card border-r border-border flex flex-col shrink-0 shadow-[var(--card-shadow)]">
        <button
          onClick={onToggle}
          className="p-2 hover:bg-accent transition-colors flex items-center justify-center"
        >
          <ChevronRight className="h-4 w-4" />
        </button>
      </aside>
    );
  }

  const hasData = coordinatorSignals.size > 0;

  // Pick a representative pair for Agent Pulse (first available, or EURUSD)
  const pulseSignal = agentSignals.get("EURUSD") ?? agentSignals.values().next().value;

  return (
    <aside
      className="bg-card border-r border-border flex flex-col shrink-0 overflow-hidden shadow-[var(--card-shadow)]"
      style={{ width: `${width}px` }}
    >
      {/* Collapse Button */}
      <div className="p-2 border-b border-border flex justify-end">
        <button
          onClick={onToggle}
          className="p-1 hover:bg-accent rounded transition-colors"
        >
          <ChevronLeft className="h-4 w-4" />
        </button>
      </div>

      {/* Watchlist Section */}
      <div className="flex-1 flex flex-col overflow-hidden">
        <div className="p-3 border-b border-border">
          <div className="flex items-center justify-between mb-2">
            <h3 className="text-xs font-semibold uppercase tracking-wider text-muted-foreground">
              Watchlist
            </h3>
            <button className="flex items-center gap-1 text-xs text-muted-foreground hover:text-foreground">
              FX Pairs <ChevronDown className="h-3 w-3" />
            </button>
          </div>
          <div className="relative">
            <Search className="absolute left-2 top-1/2 -translate-y-1/2 h-3 w-3 text-muted-foreground" />
            <Input
              placeholder="Search symbols..."
              className="h-7 text-xs pl-7 bg-muted border-0"
            />
          </div>
        </div>

        {/* Watchlist Table */}
        <div className="flex-1 overflow-auto">
          <table className="w-full text-xs">
            <thead className="sticky top-0 bg-card">
              <tr className="text-muted-foreground text-[10px] uppercase tracking-wider">
                <th className="text-left p-2 font-medium">Symbol</th>
                <th className="text-center p-2 font-medium">Dir</th>
                <th className="text-center p-2 font-medium">Conf</th>
                <th className="text-right p-2 font-medium">Action</th>
              </tr>
            </thead>
            <tbody>
              {WATCHLIST_PAIRS.map((pair) => {
                const cs = coordinatorSignals.get(pair);
                const action = toActionLabel(cs?.suggested_action ?? null);
                const conf = toConfidenceLabel(cs?.confidence_tier ?? null);
                const direction =
                  action === "BUY" ? "LONG" : action === "SELL" ? "SHORT" : "FLAT";

                return (
                  <tr
                    key={pair}
                    onClick={() => onInstrumentChange(pair)}
                    className={cn(
                      "hover:bg-accent cursor-pointer transition-colors",
                      activeInstrument === pair && "bg-accent"
                    )}
                  >
                    <td className="p-2 font-medium">{pair}</td>
                    <td className="p-2 text-center">
                      {hasData ? (
                        <Badge
                          variant="secondary"
                          className={cn(
                            "text-[9px] px-1 py-0 h-4 font-medium",
                            direction === "LONG" && "bg-[var(--long)]/10 text-[var(--long)]",
                            direction === "SHORT" && "bg-[var(--short)]/10 text-[var(--short)]",
                            direction === "FLAT" && "bg-[var(--flat)]/10 text-[var(--flat)]"
                          )}
                        >
                          {direction}
                        </Badge>
                      ) : (
                        <span className="text-muted-foreground">—</span>
                      )}
                    </td>
                    <td className="p-2 text-center font-mono text-muted-foreground">
                      {hasData ? conf : "—"}
                    </td>
                    <td className="p-2 text-right">
                      {hasData && cs && (
                        <Badge
                          className={cn(
                            "text-[9px] px-1.5 h-4",
                            action === "BUY" && "bg-[var(--buy)] text-white",
                            action === "SELL" && "bg-[var(--sell)] text-white",
                            action === "HOLD" && "bg-muted text-muted-foreground"
                          )}
                        >
                          {action}
                        </Badge>
                      )}
                    </td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>

        {/* Agent Pulse Section */}
        <div className="border-t border-border p-3">
          <h3 className="text-xs font-semibold uppercase tracking-wider text-muted-foreground mb-3">
            Agent Pulse
          </h3>
          <div className="space-y-2.5">
            {AGENT_PULSE_DEFS.map((def) => {
              const agentSig = agentSignals.get("EURUSD") ?? pulseSignal;
              const driver = def.driver(agentSig);
              const isDataReady = hasData && agentSig !== undefined;
              const status = !isDataReady ? "WARN" : "OK";

              return (
                <div key={def.name} className="bg-muted/50 rounded px-2 py-1.5">
                  <div className="flex items-center gap-1.5 text-[11px]">
                    <span className="font-medium">{def.name}</span>
                    <span className="text-muted-foreground">·</span>
                    <Badge
                      variant="secondary"
                      className={cn(
                        "text-[9px] px-1 py-0 h-3.5 font-medium",
                        status === "OK" && "bg-[var(--long)]/15 text-[var(--long)]",
                        status === "WARN" && "bg-amber-500/15 text-amber-600"
                      )}
                    >
                      {status}
                    </Badge>
                  </div>
                  <div className="flex items-center gap-1 text-[10px] text-muted-foreground mt-0.5">
                    <span>Driver:</span>
                    <span className="text-foreground font-medium truncate max-w-[100px]">{driver}</span>
                    <span className="mx-0.5">|</span>
                    <span>Impact:</span>
                    <span className="text-foreground font-medium">{def.impact}</span>
                  </div>
                </div>
              );
            })}
          </div>
        </div>
      </div>
    </aside>
  );
}
