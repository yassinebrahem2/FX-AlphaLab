"use client";

import { useState } from "react";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Tabs, TabsList, TabsTrigger, TabsContent } from "@/components/ui/tabs";
import { cn } from "@/lib/utils";

interface PairCall {
  symbol: string;
  action: "BUY" | "SELL" | "HOLD";
  conviction: number;
  positionSize: number;
  sl: string;
  tp: string;
  source: string;
  horizon: string;
}

const pairCalls: PairCall[] = [
  { symbol: "EURUSD", action: "BUY", conviction: 78, positionSize: 2.5, sl: "1.0780", tp: "1.0920", source: "Technical + Macro", horizon: "1-3 days" },
  { symbol: "GBPUSD", action: "SELL", conviction: 65, positionSize: 1.5, sl: "1.2720", tp: "1.2550", source: "Sentiment", horizon: "Intraday" },
  { symbol: "USDJPY", action: "BUY", conviction: 72, positionSize: 2.0, sl: "154.20", tp: "156.00", source: "Macro", horizon: "1-2 weeks" },
  { symbol: "AUDUSD", action: "SELL", conviction: 80, positionSize: 3.0, sl: "0.6580", tp: "0.6420", source: "Technical + Geo", horizon: "2-5 days" },
];

const agentHighlights = [
  { agent: "Technical", line1: "4H/Daily bullish, 1H neutral", line2: "Vol regime: Moderate" },
  { agent: "Macro", line1: "USD weakness from dovish Fed", line2: "Top event: NFP Friday" },
  { agent: "Geo", line1: "EU-US bilateral risk: Low", line2: "Dominant: Trade talks" },
  { agent: "Sentiment", line1: "StockTwits EUR active +42%", line2: "Stress flag: Normal" },
];

interface OrderControlsProps {
  symbol: string;
}

function OrderControls({ symbol }: OrderControlsProps) {
  const [orderType, setOrderType] = useState<"market" | "pending">("market");
  const [size, setSize] = useState("0.10");
  const [sl, setSl] = useState("");
  const [tp, setTp] = useState("");

  const bidPrice = "1.0842";
  const askPrice = "1.0844";
  const spread = "2.0";

  return (
    <div className="border-t border-border p-3 space-y-3">
      <div className="flex gap-2">
        <Button
          className="flex-1 bg-[var(--buy)] hover:bg-[var(--buy)]/90 text-white h-9"
          size="sm"
        >
          <div className="flex flex-col items-center">
            <span className="text-[10px] font-normal">BUY</span>
            <span className="font-mono text-xs">{askPrice}</span>
          </div>
        </Button>
        <Button
          className="flex-1 bg-[var(--sell)] hover:bg-[var(--sell)]/90 text-white h-9"
          size="sm"
        >
          <div className="flex flex-col items-center">
            <span className="text-[10px] font-normal">SELL</span>
            <span className="font-mono text-xs">{bidPrice}</span>
          </div>
        </Button>
      </div>

      <div className="text-center text-[10px] text-muted-foreground">
        Spread: <span className="font-mono">{spread}</span> pips
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
          <Input
            value={size}
            onChange={(e) => setSize(e.target.value)}
            className="h-7 text-xs font-mono mt-1"
          />
        </div>
        <div className="grid grid-cols-2 gap-2">
          <div>
            <label className="text-[10px] text-muted-foreground uppercase tracking-wider">Stop Loss</label>
            <Input
              value={sl}
              onChange={(e) => setSl(e.target.value)}
              placeholder="Price"
              className="h-7 text-xs font-mono mt-1"
            />
          </div>
          <div>
            <label className="text-[10px] text-muted-foreground uppercase tracking-wider">Take Profit</label>
            <Input
              value={tp}
              onChange={(e) => setTp(e.target.value)}
              placeholder="Price"
              className="h-7 text-xs font-mono mt-1"
            />
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
}

export function RightPanel({ symbol, width = 360 }: RightPanelProps) {
  const [activePairTab, setActivePairTab] = useState("EURUSD");
  const activePair = pairCalls.find((p) => p.symbol === activePairTab) || pairCalls[0];

  return (
    <aside
      className="bg-card border-l border-border flex flex-col shrink-0 overflow-hidden shadow-[var(--card-shadow)]"
      style={{ width: `${width}px` }}
    >
      {/* Alpha Assistant Header */}
      <div className="p-3 border-b border-border">
        <h2 className="text-sm font-semibold">Alpha Assistant</h2>
      </div>

      {/* Scrollable Content */}
      <div className="flex-1 overflow-auto">
        {/* Today's Call Card */}
        <div className="p-3 border-b border-border">
          <h3 className="text-xs font-semibold uppercase tracking-wider text-muted-foreground mb-3">
            {"Today's Call"}
          </h3>

          <div className="bg-muted/50 rounded-md p-3 space-y-3">
            <div className="grid grid-cols-3 gap-2 text-center">
              <div>
                <p className="text-[10px] text-muted-foreground">Top Pick</p>
                <p className="font-semibold text-sm">EURUSD</p>
              </div>
              <div>
                <p className="text-[10px] text-muted-foreground">Direction</p>
                <Badge className="bg-[var(--buy)] text-white text-[10px] px-2 h-5 mt-0.5">
                  BUY
                </Badge>
              </div>
              <div>
                <p className="text-[10px] text-muted-foreground">Conviction</p>
                <p className="font-semibold text-sm text-primary">78%</p>
              </div>
            </div>

            <div className="grid grid-cols-3 gap-2 text-center text-xs">
              <div>
                <p className="text-[10px] text-muted-foreground">Conf Tier</p>
                <p className="font-medium">High</p>
              </div>
              <div>
                <p className="text-[10px] text-muted-foreground">Pos Size</p>
                <p className="font-mono">2.5%</p>
              </div>
              <div>
                <p className="text-[10px] text-muted-foreground">SL/TP</p>
                <p className="font-mono">62/140</p>
              </div>
            </div>

            {/* Narrative */}
            <p className="text-xs text-muted-foreground leading-relaxed">
              EURUSD presents the strongest opportunity today with technical alignment across multiple timeframes.
              The 4H and Daily charts show bullish structure with price holding above the 50 EMA. Macro drivers
              favor EUR strength as the Fed turns dovish while ECB maintains hawkish stance. Retail sentiment
              is net short, providing contrarian support. Geopolitical risks remain contained with EU-US trade
              relations stable.
            </p>
          </div>
        </div>

        {/* Pair Tabs */}
        <div className="p-3 border-b border-border">
          <Tabs value={activePairTab} onValueChange={setActivePairTab}>
            <TabsList className="w-full h-7 bg-muted p-0.5">
              {pairCalls.map((pair) => (
                <TabsTrigger
                  key={pair.symbol}
                  value={pair.symbol}
                  className="flex-1 h-6 text-[10px] data-[state=active]:bg-card"
                >
                  {pair.symbol}
                </TabsTrigger>
              ))}
            </TabsList>

            <div className="mt-3 space-y-2 text-xs">
              <div className="flex justify-between">
                <span className="text-muted-foreground">Suggested Action</span>
                <Badge
                  className={cn(
                    "text-[10px] px-1.5 h-5",
                    activePair.action === "BUY" && "bg-[var(--buy)] text-white",
                    activePair.action === "SELL" && "bg-[var(--sell)] text-white",
                    activePair.action === "HOLD" && "bg-[var(--flat)] text-white"
                  )}
                >
                  {activePair.action}
                </Badge>
              </div>
              <div className="flex justify-between">
                <span className="text-muted-foreground">Conviction</span>
                <span className="font-mono">{activePair.conviction}%</span>
              </div>
              <div className="flex justify-between">
                <span className="text-muted-foreground">Position Size</span>
                <span className="font-mono">{activePair.positionSize}%</span>
              </div>
              <div className="flex justify-between">
                <span className="text-muted-foreground">SL/TP</span>
                <span className="font-mono">
                  <span className="text-[var(--short)]">{activePair.sl}</span>
                  {" / "}
                  <span className="text-[var(--long)]">{activePair.tp}</span>
                </span>
              </div>
              <div className="flex justify-between">
                <span className="text-muted-foreground">Source</span>
                <span>{activePair.source}</span>
              </div>
              <div className="flex justify-between">
                <span className="text-muted-foreground">Horizon</span>
                <span>{activePair.horizon}</span>
              </div>
            </div>
          </Tabs>
        </div>

        {/* Agent Highlights */}
        <div className="p-3 border-b border-border">
          <h3 className="text-xs font-semibold uppercase tracking-wider text-muted-foreground mb-3">
            Agent Highlights
          </h3>
          <div className="space-y-2">
            {agentHighlights.map((highlight) => (
              <div key={highlight.agent} className="text-xs">
                <span className="font-medium text-primary">{highlight.agent}:</span>
                <span className="text-muted-foreground ml-1">{highlight.line1}</span>
                <br />
                <span className="text-muted-foreground ml-[calc(0.5rem+var(--agent-width))]">
                  {highlight.line2}
                </span>
              </div>
            ))}
          </div>

          <Button variant="outline" size="sm" className="w-full mt-3 h-7 text-xs">
            Deep Dive
          </Button>
        </div>
      </div>

      {/* Order Controls - Fixed at bottom */}
      <OrderControls symbol={symbol} />
    </aside>
  );
}
