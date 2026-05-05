"use client";

import Image from "next/image";
import { FileText, Maximize2, X } from "lucide-react";
import { useMemo, useState } from "react";

import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import {
  Dialog,
  DialogClose,
  DialogContent,
  DialogHeader,
  DialogTitle,
  DialogTrigger,
} from "@/components/ui/dialog";
import { Input } from "@/components/ui/input";
import { Tabs, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { cn } from "@/lib/utils";
import { mockAgentReport } from "@/lib/agent-data";
import { getReportPath } from "@/lib/report-paths";

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

const pairCalls: PairCall[] = mockAgentReport.coordinator.per_pair.map((pair) => ({
  symbol: pair.symbol,
  action: pair.suggested_action,
  conviction: pair.conviction_score,
  positionSize: pair.position_size_pct,
  sl: `${pair.sl_pct.toFixed(2)}%`,
  tp: `${pair.tp_pct.toFixed(2)}%`,
  source: pair.direction_source,
  horizon: pair.direction_horizon,
}));

function toTechnicalDirection(value: string): string {
  if (value === "LONG") return "bullish";
  if (value === "SHORT") return "bearish";
  if (value === "FLAT") return "flat";
  return value.toLowerCase();
}

function toMacroDirection(value: string): string {
  if (value === "LONG") return "up";
  if (value === "SHORT") return "down";
  if (value === "NEUTRAL") return "neutral";
  return value.toLowerCase();
}

function toConfidenceUnit(value: number): string {
  const normalized = value > 1 ? value / 100 : value;
  return normalized.toFixed(2);
}

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
    <div className="space-y-3 border-t border-border p-3">
      <div className="flex gap-2">
        <Button className="h-9 flex-1 bg-[var(--buy)] text-white hover:bg-[var(--buy)]/90" size="sm">
          <div className="flex flex-col items-center">
            <span className="text-[10px] font-normal">BUY</span>
            <span className="font-mono text-xs">{askPrice}</span>
          </div>
        </Button>
        <Button className="h-9 flex-1 bg-[var(--sell)] text-white hover:bg-[var(--sell)]/90" size="sm">
          <div className="flex flex-col items-center">
            <span className="text-[10px] font-normal">SELL</span>
            <span className="font-mono text-xs">{bidPrice}</span>
          </div>
        </Button>
      </div>

      <div className="text-center text-[10px] text-muted-foreground">
        {symbol} spread: <span className="font-mono">{spread}</span> pips
      </div>

      <div className="flex gap-2">
        <Button
          variant={orderType === "market" ? "default" : "outline"}
          size="sm"
          className="h-7 flex-1 text-xs"
          onClick={() => setOrderType("market")}
        >
          Market
        </Button>
        <Button
          variant={orderType === "pending" ? "default" : "outline"}
          size="sm"
          className="h-7 flex-1 text-xs"
          onClick={() => setOrderType("pending")}
        >
          Pending
        </Button>
      </div>

      <div className="space-y-2">
        <div>
          <label className="text-[10px] uppercase tracking-wider text-muted-foreground">Size (lots)</label>
          <Input value={size} onChange={(e) => setSize(e.target.value)} className="mt-1 h-7 text-xs font-mono" />
        </div>
        <div className="grid grid-cols-2 gap-2">
          <div>
            <label className="text-[10px] uppercase tracking-wider text-muted-foreground">Stop Loss</label>
            <Input
              value={sl}
              onChange={(e) => setSl(e.target.value)}
              placeholder="Price"
              className="mt-1 h-7 text-xs font-mono"
            />
          </div>
          <div>
            <label className="text-[10px] uppercase tracking-wider text-muted-foreground">Take Profit</label>
            <Input
              value={tp}
              onChange={(e) => setTp(e.target.value)}
              placeholder="Price"
              className="mt-1 h-7 text-xs font-mono"
            />
          </div>
        </div>
      </div>

      <Button className="h-8 w-full text-xs">Place Order</Button>
    </div>
  );
}

interface RightPanelProps {
  symbol: string;
  width?: number;
}

export function RightPanel({ symbol, width = 340 }: RightPanelProps) {
  const [activePairTab, setActivePairTab] = useState(symbol);
  const [reportOpen, setReportOpen] = useState(false);

  const activeSymbol = useMemo(() => {
    if (pairCalls.some((pair) => pair.symbol === symbol)) {
      return activePairTab === symbol || !pairCalls.some((pair) => pair.symbol === activePairTab)
        ? symbol
        : activePairTab;
    }

    return activePairTab;
  }, [activePairTab, symbol]);

  const activePair = pairCalls.find((pair) => pair.symbol === activeSymbol) ?? pairCalls[0];
  const topPick =
    mockAgentReport.coordinator.per_pair.find((pair) => pair.symbol === mockAgentReport.coordinator.top_pick) ??
    mockAgentReport.coordinator.per_pair[0];
  const technical = mockAgentReport.technical.find((item) => item.symbol === activePair.symbol);
  const macro = mockAgentReport.macro.find((item) => item.symbol === activePair.symbol);
  const geo = mockAgentReport.geopolitical.find((item) => item.symbol === activePair.symbol);
  const sentiment = mockAgentReport.sentiment[0];
  const reportHref = getReportPath(activePair.symbol);

  return (
    <aside
      className="flex shrink-0 flex-col overflow-hidden border-l border-border bg-card shadow-[var(--card-shadow)]"
      style={{ width: `${width}px` }}
    >
      <div className="border-b border-border px-3 py-3">
        <div className="flex items-center gap-2.5">
          <div className="flex h-9 w-12 items-center justify-center overflow-hidden rounded-sm bg-muted/40 px-1">
            <Image src="/fx-mark1.png" alt="FX AlphaLab logo" width={44} height={28} className="h-auto w-full" />
          </div>
          <div>
            <p className="text-[10px] uppercase tracking-[0.18em] text-muted-foreground">Alpha Signal</p>
            <h2 className="text-sm font-semibold">AI Recommendation</h2>
          </div>
        </div>
      </div>

      <div className="flex-1 overflow-auto">
        <div className="border-b border-border p-3">
          <h3 className="mb-3 text-xs font-semibold uppercase tracking-wider text-muted-foreground">
            Today&apos;s Call
          </h3>

          <div className="rounded-xl border border-primary/15 bg-[linear-gradient(180deg,rgba(31,74,168,0.12),rgba(31,74,168,0.03))] p-4 shadow-sm">
            <div className="mb-4 flex items-start justify-between gap-3">
              <div>
                <p className="text-[10px] uppercase tracking-[0.18em] text-muted-foreground">Top Pick</p>
                <div className="mt-1 flex items-center gap-2">
                  <p className="text-xl font-semibold text-foreground">{topPick.symbol}</p>
                  <Badge
                    className={cn(
                      "h-6 px-2.5 text-[10px] text-white",
                      mockAgentReport.coordinator.overall_action === "BUY" && "bg-[var(--buy)]",
                      mockAgentReport.coordinator.overall_action === "SELL" && "bg-[var(--sell)]",
                      mockAgentReport.coordinator.overall_action === "HOLD" && "bg-[var(--flat)]"
                    )}
                  >
                    {mockAgentReport.coordinator.overall_action}
                  </Badge>
                </div>
              </div>
              <div className="text-right">
                <p className="text-[10px] uppercase tracking-[0.18em] text-muted-foreground">Conviction</p>
                <p className="mt-1 text-2xl font-semibold text-primary">{topPick.conviction_score}%</p>
              </div>
            </div>

            <div className="grid grid-cols-3 gap-2 text-center text-xs">
              <div className="rounded-lg border border-border/60 bg-background/75 px-2 py-2">
                <p className="text-[10px] text-muted-foreground">Conf Tier</p>
                <p className="mt-1 font-medium">{topPick.confidence_tier}</p>
              </div>
              <div className="rounded-lg border border-border/60 bg-background/75 px-2 py-2">
                <p className="text-[10px] text-muted-foreground">Pos Size</p>
                <p className="mt-1 font-mono">{topPick.position_size_pct}%</p>
              </div>
              <div className="rounded-lg border border-border/60 bg-background/75 px-2 py-2">
                <p className="text-[10px] text-muted-foreground">SL/TP</p>
                <p className="mt-1 font-mono">
                  {topPick.sl_pct.toFixed(2)}% / {topPick.tp_pct.toFixed(2)}%
                </p>
              </div>
            </div>

            <div className="mt-4 rounded-lg border border-border/60 bg-background/70 px-3 py-3">
              <p className="text-[10px] uppercase tracking-[0.18em] text-muted-foreground">Context</p>
              <p className="mt-2 text-xs leading-relaxed text-muted-foreground">
                {mockAgentReport.coordinator.narrative_context}
              </p>
            </div>

            <Dialog open={reportOpen} onOpenChange={setReportOpen}>
              <DialogTrigger asChild>
                <Button
                  size="sm"
                  className="mt-3 h-9 w-full border border-primary/30 bg-primary text-xs font-semibold text-primary-foreground shadow-sm hover:bg-primary/90"
                >
                  <FileText className="mr-1.5 h-3.5 w-3.5" />
                  Open Deep Dive Report
                </Button>
              </DialogTrigger>
              <DialogContent
                showCloseButton={false}
                className="flex h-[94vh] max-h-[94vh] w-[96vw] max-w-none grid-rows-none flex-col gap-0 overflow-hidden rounded-xl border border-border bg-[#e8eaef] p-0 shadow-2xl sm:!max-w-[1500px]"
              >
                <DialogHeader className="shrink-0 border-b border-border bg-card px-5 py-3.5">
                  <div className="flex items-center justify-between gap-4">
                    <div className="flex min-w-0 items-center gap-3">
                      <div className="flex h-9 w-12 shrink-0 items-center justify-center overflow-hidden rounded-md bg-muted/50 px-1">
                        <Image
                          src="/fx-mark1.png"
                          alt="FX AlphaLab logo"
                          width={28}
                          height={28}
                          className="h-7 w-7 rounded-md"
                        />
                      </div>
                      <div className="min-w-0">
                        <DialogTitle className="text-base">Deep Dive Report</DialogTitle>
                      </div>
                    </div>
                    <div className="flex shrink-0 items-center gap-2">
                      <a
                        href={reportHref}
                        target="_blank"
                        rel="noreferrer"
                        className="inline-flex h-8 items-center gap-2 rounded-md border border-primary/25 bg-primary/10 px-3 text-[11px] font-semibold text-primary transition-colors hover:border-primary/45 hover:bg-primary/15"
                      >
                        <Maximize2 className="h-3.5 w-3.5" />
                        Full view
                      </a>
                      <DialogClose className="inline-flex h-8 w-8 items-center justify-center rounded-md border border-border bg-background text-muted-foreground transition-colors hover:bg-muted hover:text-foreground">
                        <X className="h-4 w-4" />
                        <span className="sr-only">Close</span>
                      </DialogClose>
                    </div>
                  </div>
                </DialogHeader>
                <div className="min-h-0 flex-1 overflow-hidden bg-[#dfe3ea] p-3">
                  <iframe
                    src={reportHref}
                    title={`${activePair.symbol} deep dive report`}
                    className="block h-full w-full rounded-lg border border-border bg-[#eee9df] shadow-sm"
                  />
                </div>
              </DialogContent>
            </Dialog>
          </div>
        </div>

        <div className="border-b border-border p-3">
          <h3 className="mb-3 text-xs font-semibold uppercase tracking-wider text-muted-foreground">
            Recommendation Details
          </h3>
          <Tabs value={activePair.symbol} onValueChange={setActivePairTab}>
            <TabsList className="h-7 w-full bg-muted p-0.5">
              {pairCalls.map((pair) => (
                <TabsTrigger
                  key={pair.symbol}
                  value={pair.symbol}
                  className="h-6 flex-1 text-[10px] data-[state=active]:bg-card"
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
                    "h-5 px-1.5 text-[10px]",
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
                  <span className="text-[var(--short)]">{activePair.sl}</span> /{" "}
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

        <div className="border-b border-border p-3">
          <h3 className="mb-3 text-xs font-semibold uppercase tracking-wider text-muted-foreground">
            Actual Agent Outputs
          </h3>
          <div className="space-y-2.5 text-xs">
            <div className="rounded-md border border-border/70 bg-muted/25 px-3 py-2.5">
              <div className="text-[10px] uppercase tracking-[0.16em] text-muted-foreground">Technical</div>
              <div className="mt-1.5 grid grid-cols-2 gap-x-3 gap-y-1">
                <div className="text-muted-foreground">direction</div>
                <div className="font-medium text-foreground">{toTechnicalDirection(technical?.direction ?? "FLAT")}</div>
                <div className="text-muted-foreground">confidence</div>
                <div className="font-mono text-foreground">{toConfidenceUnit(technical?.confidence ?? 0)}</div>
                <div className="text-muted-foreground">timeframe_votes</div>
                <div className="font-medium text-foreground">
                  {technical?.timeframe_votes.map((vote) => `${vote.timeframe}:${vote.direction}`).join(" | ") ?? "N/A"}
                </div>
                <div className="text-muted-foreground">volatility_regime</div>
                <div className="font-medium text-foreground">{technical?.volatility_regime.toLowerCase() ?? "n/a"}</div>
              </div>
            </div>

            <div className="rounded-md border border-border/70 bg-muted/25 px-3 py-2.5">
              <div className="text-[10px] uppercase tracking-[0.16em] text-muted-foreground">Macro</div>
              <div className="mt-1.5 grid grid-cols-2 gap-x-3 gap-y-1">
                <div className="text-muted-foreground">module_c_direction</div>
                <div className="font-medium text-foreground">{toMacroDirection(macro?.module_c_direction ?? "NEUTRAL")}</div>
                <div className="text-muted-foreground">macro_confidence</div>
                <div className="font-mono text-foreground">{toConfidenceUnit(macro?.macro_confidence ?? 0)}</div>
                <div className="text-muted-foreground">dominant_driver</div>
                <div className="font-medium text-foreground">{macro?.dominant_driver ?? "N/A"}</div>
                <div className="text-muted-foreground">surprise | bias</div>
                <div className="font-mono text-foreground">
                  {macro?.macro_surprise_score.toFixed(2) ?? "0.00"} | {macro?.macro_bias_score.toFixed(2) ?? "0.00"}
                </div>
              </div>
            </div>

            <div className="rounded-md border border-border/70 bg-muted/25 px-3 py-2.5">
              <div className="text-[10px] uppercase tracking-[0.16em] text-muted-foreground">Geo</div>
              <div className="mt-1.5 grid grid-cols-2 gap-x-3 gap-y-1">
                <div className="text-muted-foreground">risk_regime</div>
                <div className="font-medium text-foreground">{geo?.risk_regime.toLowerCase() ?? "n/a"}</div>
                <div className="text-muted-foreground">bilateral_risk_score</div>
                <div className="font-mono text-foreground">{geo?.bilateral_risk_score.toFixed(2) ?? "0.00"}</div>
                <div className="text-muted-foreground">base_driver</div>
                <div className="font-medium text-foreground">{geo?.base_dominant_driver ?? "N/A"}</div>
                <div className="text-muted-foreground">quote_driver</div>
                <div className="font-medium text-foreground">{geo?.quote_dominant_driver ?? "N/A"}</div>
              </div>
            </div>

            <div className="rounded-md border border-border/70 bg-muted/25 px-3 py-2.5">
              <div className="text-[10px] uppercase tracking-[0.16em] text-muted-foreground">Sentiment</div>
              <div className="mt-1.5 grid grid-cols-2 gap-x-3 gap-y-1">
                <div className="text-muted-foreground">usdjpy_stocktwits_active</div>
                <div className="font-medium text-foreground">{sentiment?.usdjpy_stocktwits_active ? "true" : "false"}</div>
                <div className="text-muted-foreground">composite_stress_flag</div>
                <div className="font-medium text-foreground">{sentiment?.composite_stress_flag ? "true" : "false"}</div>
                <div className="text-muted-foreground">stress_sources</div>
                <div className="font-medium text-foreground">{sentiment?.stress_sources.join(", ") || "[]"}</div>
                <div className="text-muted-foreground">attention_zscores</div>
                <div className="font-mono text-foreground">
                  {sentiment?.gdelt_attention_zscore.toFixed(2) ?? "0.00"} | {sentiment?.macro_attention_zscore.toFixed(2) ?? "0.00"}
                </div>
              </div>
            </div>
            <div className="grid grid-cols-2 gap-2">
              <div className="rounded-md border border-border/70 bg-muted/25 px-3 py-2">
                <div className="text-[10px] uppercase tracking-[0.16em] text-muted-foreground">Direction Source</div>
                <div className="mt-1 font-medium text-foreground">{activePair.source}</div>
              </div>
              <div className="rounded-md border border-border/70 bg-muted/25 px-3 py-2">
                <div className="text-[10px] uppercase tracking-[0.16em] text-muted-foreground">Horizon</div>
                <div className="mt-1 font-medium text-foreground">{activePair.horizon}</div>
              </div>
            </div>
          </div>
        </div>
      </div>

      <OrderControls symbol={activePair.symbol} />
    </aside>
  );
}
