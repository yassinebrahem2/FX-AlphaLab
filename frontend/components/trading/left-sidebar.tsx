"use client";

import { Search, ChevronDown, ChevronLeft, ChevronRight } from "lucide-react";
import { Input } from "@/components/ui/input";
import { Badge } from "@/components/ui/badge";
import { cn } from "@/lib/utils";

interface WatchlistItem {
  symbol: string;
  direction: "LONG" | "SHORT" | "FLAT";
  confidence: string;
  bid: string;
  ask: string;
}

const watchlist: WatchlistItem[] = [
  { symbol: "EURUSD", direction: "LONG", confidence: "3/3", bid: "1.0842", ask: "1.0844" },
  { symbol: "GBPUSD", direction: "SHORT", confidence: "2/3", bid: "1.2651", ask: "1.2653" },
  { symbol: "USDJPY", direction: "LONG", confidence: "2/3", bid: "154.82", ask: "154.84" },
  { symbol: "USDCHF", direction: "FLAT", confidence: "1/3", bid: "0.8842", ask: "0.8844" },
  { symbol: "AUDUSD", direction: "SHORT", confidence: "3/3", bid: "0.6512", ask: "0.6514" },
  { symbol: "NZDUSD", direction: "LONG", confidence: "2/3", bid: "0.5982", ask: "0.5984" },
  { symbol: "USDCAD", direction: "FLAT", confidence: "1/3", bid: "1.3652", ask: "1.3654" },
];

interface AgentPulse {
  name: string;
  status: "OK" | "WARN" | "ALERT";
  freshness: string;
  driver: string;
  impact: string;
  delta: "up" | "down" | "flat";
}

const agentPulse: AgentPulse[] = [
  { name: "Technical", status: "OK", freshness: "5m ago", driver: "momentum", impact: "entry (1d)", delta: "up" },
  { name: "Macro", status: "OK", freshness: "2h ago", driver: "carry", impact: "direction (5d)", delta: "up" },
  { name: "Geopolitical", status: "WARN", freshness: "1d ago", driver: "tariff risk", impact: "volatility (2w)", delta: "down" },
  { name: "Sentiment", status: "OK", freshness: "15m ago", driver: "positioning", impact: "reversal (3d)", delta: "flat" },
];

interface LeftSidebarProps {
  collapsed: boolean;
  onToggle: () => void;
  activeInstrument: string;
  onInstrumentChange: (symbol: string) => void;
  width?: number;
}

export function LeftSidebar({ collapsed, onToggle, activeInstrument, onInstrumentChange, width = 260 }: LeftSidebarProps) {
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
              Favorites <ChevronDown className="h-3 w-3" />
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
                <th className="text-right p-2 font-medium">Bid</th>
                <th className="text-right p-2 font-medium">Ask</th>
              </tr>
            </thead>
            <tbody>
              {watchlist.map((item) => (
                <tr
                  key={item.symbol}
                  onClick={() => onInstrumentChange(item.symbol)}
                  className={cn(
                    "hover:bg-accent cursor-pointer transition-colors",
                    activeInstrument === item.symbol && "bg-accent"
                  )}
                >
                  <td className="p-2 font-medium">{item.symbol}</td>
                  <td className="p-2 text-center">
                    <Badge
                      variant="secondary"
                      className={cn(
                        "text-[9px] px-1 py-0 h-4 font-medium",
                        item.direction === "LONG" && "bg-[var(--long)]/10 text-[var(--long)]",
                        item.direction === "SHORT" && "bg-[var(--short)]/10 text-[var(--short)]",
                        item.direction === "FLAT" && "bg-[var(--flat)]/10 text-[var(--flat)]"
                      )}
                    >
                      {item.direction}
                    </Badge>
                  </td>
                  <td className="p-2 text-center font-mono text-muted-foreground">
                    {item.confidence}
                  </td>
                  <td className="p-2 text-right font-mono">{item.bid}</td>
                  <td className="p-2 text-right font-mono">{item.ask}</td>
                </tr>
              ))}
            </tbody>
          </table>
          <div className="px-2 pb-2">
            <button className="text-[10px] text-primary hover:underline">why</button>
          </div>
        </div>

        {/* Agent Pulse Section */}
        <div className="border-t border-border p-3">
          <h3 className="text-xs font-semibold uppercase tracking-wider text-muted-foreground mb-3">
            Agent Pulse
          </h3>
          <div className="space-y-2.5">
            {agentPulse.map((agent) => (
              <div key={agent.name} className="bg-muted/50 rounded px-2 py-1.5">
                {/* Line 1: Agent name + status badge + freshness + delta */}
                <div className="flex items-center gap-1.5 text-[11px]">
                  <span className="font-medium">{agent.name}</span>
                  <span className="text-muted-foreground">·</span>
                  <span className="text-muted-foreground">{agent.freshness}</span>
                  <span className="text-muted-foreground">·</span>
                  <Badge
                    variant="secondary"
                    className={cn(
                      "text-[9px] px-1 py-0 h-3.5 font-medium",
                      agent.status === "OK" && "bg-[var(--long)]/15 text-[var(--long)]",
                      agent.status === "WARN" && "bg-amber-500/15 text-amber-600",
                      agent.status === "ALERT" && "bg-[var(--short)]/15 text-[var(--short)]"
                    )}
                  >
                    {agent.status}
                  </Badge>
                  <span className={cn(
                    "ml-auto text-[10px] font-medium",
                    agent.delta === "up" && "text-[var(--long)]",
                    agent.delta === "down" && "text-[var(--short)]",
                    agent.delta === "flat" && "text-muted-foreground"
                  )}>
                    {agent.delta === "up" && "↑"}
                    {agent.delta === "down" && "↓"}
                    {agent.delta === "flat" && "→"}
                  </span>
                </div>
                {/* Line 2: Key driver + impact */}
                <div className="flex items-center gap-1 text-[10px] text-muted-foreground mt-0.5">
                  <span>Driver:</span>
                  <span className="text-foreground font-medium">{agent.driver}</span>
                  <span className="mx-0.5">|</span>
                  <span>Impact:</span>
                  <span className="text-foreground font-medium">{agent.impact}</span>
                </div>
              </div>
            ))}
          </div>
        </div>
      </div>
    </aside>
  );
}
