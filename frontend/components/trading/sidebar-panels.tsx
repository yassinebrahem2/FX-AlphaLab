"use client";

import { ArrowDownRight, ArrowRight, ArrowUpRight, ChevronLeft, Search } from "lucide-react";

import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { ScrollArea } from "@/components/ui/scroll-area";
import { cn } from "@/lib/utils";

export interface WatchlistItem {
  symbol: string;
  direction: "LONG" | "SHORT" | "FLAT";
  confidence: string;
  bid: string;
  ask: string;
}

export interface AgentPulse {
  name: string;
  freshness: string;
  primaryOutput: string;
  secondaryOutput: string;
  insight: string;
}

interface WatchlistPanelProps {
  items: WatchlistItem[];
  activeInstrument: string;
  onInstrumentChange: (symbol: string) => void;
  onToggle?: () => void;
}

interface WatchlistRowProps {
  item: WatchlistItem;
  active?: boolean;
  onSelect: (symbol: string) => void;
}

interface AgentPulsePanelProps {
  items: AgentPulse[];
}

interface AgentPulseCardProps {
  item: AgentPulse;
}

interface DirectionBadgeProps {
  direction: WatchlistItem["direction"];
}

export function DirectionBadge({ direction }: DirectionBadgeProps) {
  if (direction === "LONG") {
    return (
      <Badge className="h-4 rounded-full border border-emerald-200 bg-emerald-50 px-1.5 text-[8px] font-medium text-emerald-700">
        <ArrowUpRight className="mr-0.5 h-2.5 w-2.5" />
        LONG
      </Badge>
    );
  }

  if (direction === "SHORT") {
    return (
      <Badge className="h-4 rounded-full border border-rose-200 bg-rose-50 px-1.5 text-[8px] font-medium text-rose-700">
        <ArrowDownRight className="mr-0.5 h-2.5 w-2.5" />
        SHORT
      </Badge>
    );
  }

  return (
    <Badge className="h-4 rounded-full border border-slate-200 bg-slate-50 px-1.5 text-[8px] font-medium text-slate-600">
      <ArrowRight className="mr-0.5 h-2.5 w-2.5" />
      FLAT
    </Badge>
  );
}

export function WatchlistRow({ item, active = false, onSelect }: WatchlistRowProps) {
  return (
    <button
      onClick={() => onSelect(item.symbol)}
      className={cn(
        "grid h-14 w-full grid-cols-[minmax(0,1fr)_auto_auto] items-center gap-2 rounded-lg border border-transparent px-2.5 py-2 text-left transition-colors hover:border-border/70 hover:bg-muted/70",
        active && "bg-muted/80"
      )}
    >
      <div className="min-w-0">
        <div className="truncate text-[12px] font-medium leading-tight text-foreground">{item.symbol}</div>
        <div className="truncate text-[9px] leading-tight text-muted-foreground">Major FX pair</div>
      </div>

      <div className="flex flex-col items-center gap-0.5">
        <DirectionBadge direction={item.direction} />
        <span className="text-[9px] font-medium leading-none text-muted-foreground">{item.confidence}</span>
      </div>

      <div className="min-w-[4.25rem] text-right">
        <div className="font-mono tabular-nums text-[11px] leading-tight text-foreground">{item.bid}</div>
        <div className="font-mono tabular-nums text-[9px] leading-tight text-muted-foreground">{item.ask}</div>
      </div>
    </button>
  );
}

export function WatchlistPanel({ items, activeInstrument, onInstrumentChange, onToggle }: WatchlistPanelProps) {
  return (
    <Card className="gap-0 rounded-none border-0 border-b border-border bg-card py-0 shadow-none">
      <CardHeader className="px-3 py-2 pb-1.5">
        <div className="flex items-center justify-between gap-3">
          <div className="flex min-w-0 items-start gap-2">
            {onToggle ? (
              <button
                onClick={onToggle}
                className="mt-0.5 rounded-md p-1 text-muted-foreground transition-colors hover:bg-muted hover:text-foreground"
                aria-label="Collapse sidebar"
              >
                <ChevronLeft className="h-4 w-4" />
              </button>
            ) : null}
            <div className="min-w-0">
              <CardTitle className="text-sm font-semibold text-foreground">Watchlist</CardTitle>
              <p className="mt-0.5 text-[10px] leading-tight text-muted-foreground">Live pair quotes and direction</p>
            </div>
          </div>
        </div>
        <div className="relative pt-0.25">
          <Search className="absolute left-3 top-1/2 h-3.5 w-3.5 -translate-y-1/2 text-muted-foreground" />
          <Input placeholder="Search symbols..." className="h-9 border-border bg-background pl-9 text-xs" />
        </div>
      </CardHeader>
      <CardContent className="px-0 pb-0">
        <ScrollArea className="h-[10.5rem]">
          <div className="space-y-1 px-2 pb-2 pt-1">
            {items.map((item) => (
              <WatchlistRow
                key={item.symbol}
                item={item}
                active={activeInstrument === item.symbol}
                onSelect={onInstrumentChange}
              />
            ))}
          </div>
        </ScrollArea>
      </CardContent>
    </Card>
  );
}

export function AgentPulseCard({ item }: AgentPulseCardProps) {
  return (
    <Card className="gap-0 rounded-lg border-border/70 bg-muted/20 py-0 shadow-none transition-colors hover:bg-muted/35">
      <CardContent className="px-2.5 py-1.5">
        <div className="flex items-start justify-between gap-1.5">
          <div className="min-w-0 flex-1">
            <div className="flex items-center gap-1 text-[10px] leading-none">
              <span className="truncate font-semibold text-foreground">{item.name}</span>
              <span className="text-[9px] text-muted-foreground">{item.freshness}</span>
            </div>
          </div>
          <div className="flex shrink-0 items-center gap-1">
            <Badge className="h-4 rounded-full border border-border/70 bg-background px-1.5 text-[8px] font-medium text-foreground">
              {item.primaryOutput}
            </Badge>
            <Badge variant="outline" className="h-4 rounded-full px-1.5 text-[8px] font-medium text-muted-foreground">
              {item.secondaryOutput}
            </Badge>
          </div>
        </div>

        <div className="mt-1.5 text-[10px] leading-tight text-muted-foreground">
          {item.insight}
        </div>
      </CardContent>
    </Card>
  );
}

export function AgentPulsePanel({ items }: AgentPulsePanelProps) {
  return (
    <Card className="min-h-0 flex-1 overflow-hidden gap-0 rounded-none border-0 bg-card py-0 shadow-none">
      <CardHeader className="px-3 py-2 pb-1.5">
        <CardTitle className="text-sm font-semibold text-foreground">Agent Pulse</CardTitle>
        <p className="text-[10px] leading-tight text-muted-foreground">Desk view of current agent state</p>
      </CardHeader>
      <CardContent className="min-h-0 flex-1 overflow-hidden px-3 pb-2 pt-0">
        <div className="h-full space-y-1">
          {items.map((item) => (
            <AgentPulseCard key={item.name} item={item} />
          ))}
        </div>
      </CardContent>
    </Card>
  );
}
