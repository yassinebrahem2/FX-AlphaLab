"use client";

import { useState } from "react";
import { X } from "lucide-react";
import { Tabs, TabsList, TabsTrigger, TabsContent } from "@/components/ui/tabs";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { cn } from "@/lib/utils";

interface Position {
  id: string;
  symbol: string;
  direction: "BUY" | "SELL";
  size: number;
  entryPrice: string;
  currentBid: string;
  currentAsk: string;
  pnl: number;
  sl: string;
  tp: string;
}

const openPositions: Position[] = [
  { id: "1", symbol: "EURUSD", direction: "BUY", size: 0.5, entryPrice: "1.0820", currentBid: "1.0842", currentAsk: "1.0844", pnl: 110.00, sl: "1.0780", tp: "1.0920" },
  { id: "2", symbol: "GBPUSD", direction: "SELL", size: 0.3, entryPrice: "1.2680", currentBid: "1.2651", currentAsk: "1.2653", pnl: 87.00, sl: "1.2720", tp: "1.2580" },
  { id: "3", symbol: "AUDUSD", direction: "SELL", size: 0.2, entryPrice: "0.6540", currentBid: "0.6512", currentAsk: "0.6514", pnl: 56.00, sl: "0.6580", tp: "0.6450" },
];

const pendingOrders: Position[] = [
  { id: "4", symbol: "USDJPY", direction: "BUY", size: 0.4, entryPrice: "154.50", currentBid: "154.82", currentAsk: "154.84", pnl: 0, sl: "154.00", tp: "156.00" },
];

const closedPositions: Position[] = [
  { id: "5", symbol: "EURUSD", direction: "BUY", size: 0.5, entryPrice: "1.0750", currentBid: "1.0820", currentAsk: "1.0822", pnl: 350.00, sl: "1.0710", tp: "1.0820" },
  { id: "6", symbol: "GBPUSD", direction: "SELL", size: 0.2, entryPrice: "1.2700", currentBid: "1.2720", currentAsk: "1.2722", pnl: -40.00, sl: "1.2720", tp: "1.2650" },
];

function PositionRow({ position, showClose = true }: { position: Position; showClose?: boolean }) {
  const isProfitable = position.pnl >= 0;

  return (
    <tr className="hover:bg-accent/50 transition-colors">
      <td className="p-2 font-medium">{position.symbol}</td>
      <td className="p-2">
        <Badge
          variant="secondary"
          className={cn(
            "text-[9px] px-1.5 h-4",
            position.direction === "BUY" && "bg-[var(--buy)]/10 text-[var(--buy)]",
            position.direction === "SELL" && "bg-[var(--sell)]/10 text-[var(--sell)]"
          )}
        >
          {position.direction}
        </Badge>
      </td>
      <td className="p-2 font-mono text-right">{position.size.toFixed(2)}</td>
      <td className="p-2 font-mono text-right">{position.entryPrice}</td>
      <td className="p-2 font-mono text-right">{position.currentBid}</td>
      <td className="p-2 font-mono text-right">{position.currentAsk}</td>
      <td className={cn(
        "p-2 font-mono text-right font-medium",
        isProfitable ? "text-[var(--profit)]" : "text-[var(--loss)]"
      )}>
        {isProfitable ? "+" : ""}{position.pnl.toFixed(2)}
      </td>
      <td className="p-2 font-mono text-right text-[var(--short)]">{position.sl}</td>
      <td className="p-2 font-mono text-right text-[var(--long)]">{position.tp}</td>
      {showClose && (
        <td className="p-2">
          <Button
            variant="ghost"
            size="icon"
            className="h-5 w-5 text-muted-foreground hover:text-destructive"
          >
            <X className="h-3 w-3" />
          </Button>
        </td>
      )}
    </tr>
  );
}

function EmptyState({ message }: { message: string }) {
  return (
    <div className="flex items-center justify-center h-full text-muted-foreground text-sm">
      {message}
    </div>
  );
}

interface BottomPanelProps {
  height?: number;
}

export function BottomPanel({ height = 190 }: BottomPanelProps) {
  const [activeTab, setActiveTab] = useState("open");

  return (
    <div
      className="bg-card border-t border-border shrink-0 flex flex-col shadow-[var(--card-shadow)]"
      style={{ height: `${height}px` }}
    >
      <Tabs value={activeTab} onValueChange={setActiveTab} className="flex-1 flex flex-col">
        <div className="border-b border-border px-3">
          <TabsList className="h-9 bg-transparent p-0 gap-4">
            <TabsTrigger
              value="open"
              className="h-9 px-0 pb-0 data-[state=active]:shadow-none data-[state=active]:bg-transparent rounded-none border-b-2 border-transparent data-[state=active]:border-primary text-xs"
            >
              OPEN
              <Badge variant="secondary" className="ml-1.5 h-4 px-1 text-[10px]">
                {openPositions.length}
              </Badge>
            </TabsTrigger>
            <TabsTrigger
              value="pending"
              className="h-9 px-0 pb-0 data-[state=active]:shadow-none data-[state=active]:bg-transparent rounded-none border-b-2 border-transparent data-[state=active]:border-primary text-xs"
            >
              PENDING
              <Badge variant="secondary" className="ml-1.5 h-4 px-1 text-[10px]">
                {pendingOrders.length}
              </Badge>
            </TabsTrigger>
            <TabsTrigger
              value="closed"
              className="h-9 px-0 pb-0 data-[state=active]:shadow-none data-[state=active]:bg-transparent rounded-none border-b-2 border-transparent data-[state=active]:border-primary text-xs"
            >
              CLOSED
              <Badge variant="secondary" className="ml-1.5 h-4 px-1 text-[10px]">
                {closedPositions.length}
              </Badge>
            </TabsTrigger>
          </TabsList>
        </div>

        <TabsContent value="open" className="flex-1 m-0 overflow-auto">
          {openPositions.length === 0 ? (
            <EmptyState message="No open positions." />
          ) : (
            <table className="w-full text-xs">
              <thead className="sticky top-0 bg-card">
                <tr className="text-muted-foreground text-[10px] uppercase tracking-wider border-b border-border">
                  <th className="text-left p-2 font-medium">Symbol</th>
                  <th className="text-left p-2 font-medium">Dir</th>
                  <th className="text-right p-2 font-medium">Size</th>
                  <th className="text-right p-2 font-medium">Entry</th>
                  <th className="text-right p-2 font-medium">Bid</th>
                  <th className="text-right p-2 font-medium">Ask</th>
                  <th className="text-right p-2 font-medium">P&L</th>
                  <th className="text-right p-2 font-medium">SL</th>
                  <th className="text-right p-2 font-medium">TP</th>
                  <th className="p-2 w-8"></th>
                </tr>
              </thead>
              <tbody>
                {openPositions.map((position) => (
                  <PositionRow key={position.id} position={position} />
                ))}
              </tbody>
            </table>
          )}
        </TabsContent>

        <TabsContent value="pending" className="flex-1 m-0 overflow-auto">
          {pendingOrders.length === 0 ? (
            <EmptyState message="No pending orders." />
          ) : (
            <table className="w-full text-xs">
              <thead className="sticky top-0 bg-card">
                <tr className="text-muted-foreground text-[10px] uppercase tracking-wider border-b border-border">
                  <th className="text-left p-2 font-medium">Symbol</th>
                  <th className="text-left p-2 font-medium">Dir</th>
                  <th className="text-right p-2 font-medium">Size</th>
                  <th className="text-right p-2 font-medium">Price</th>
                  <th className="text-right p-2 font-medium">Bid</th>
                  <th className="text-right p-2 font-medium">Ask</th>
                  <th className="text-right p-2 font-medium">P&L</th>
                  <th className="text-right p-2 font-medium">SL</th>
                  <th className="text-right p-2 font-medium">TP</th>
                  <th className="p-2 w-8"></th>
                </tr>
              </thead>
              <tbody>
                {pendingOrders.map((position) => (
                  <PositionRow key={position.id} position={position} />
                ))}
              </tbody>
            </table>
          )}
        </TabsContent>

        <TabsContent value="closed" className="flex-1 m-0 overflow-auto">
          {closedPositions.length === 0 ? (
            <EmptyState message="No closed positions." />
          ) : (
            <table className="w-full text-xs">
              <thead className="sticky top-0 bg-card">
                <tr className="text-muted-foreground text-[10px] uppercase tracking-wider border-b border-border">
                  <th className="text-left p-2 font-medium">Symbol</th>
                  <th className="text-left p-2 font-medium">Dir</th>
                  <th className="text-right p-2 font-medium">Size</th>
                  <th className="text-right p-2 font-medium">Entry</th>
                  <th className="text-right p-2 font-medium">Bid</th>
                  <th className="text-right p-2 font-medium">Ask</th>
                  <th className="text-right p-2 font-medium">P&L</th>
                  <th className="text-right p-2 font-medium">SL</th>
                  <th className="text-right p-2 font-medium">TP</th>
                </tr>
              </thead>
              <tbody>
                {closedPositions.map((position) => (
                  <PositionRow key={position.id} position={position} showClose={false} />
                ))}
              </tbody>
            </table>
          )}
        </TabsContent>
      </Tabs>
    </div>
  );
}
