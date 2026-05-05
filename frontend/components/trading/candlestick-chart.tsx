"use client";

import { useEffect, useRef, useState } from "react";
import {
  createChart,
  ColorType,
  IChartApi,
  CandlestickData,
  Time,
  CandlestickSeries,
} from "lightweight-charts";
import { Badge } from "@/components/ui/badge";
import { cn } from "@/lib/utils";
import { useOhlcv } from "@/hooks/use-ohlcv";
import { CoordinatorReportAPI, CoordinatorSignalAPI, toActionLabel } from "@/lib/api";

interface TooltipData {
  time: string;
  open: string;
  high: string;
  low: string;
  close: string;
  rsi: string | null;
  macd: string | null;
  bbPercent: string | null;
  ema200: boolean | null;
  atrRank: string | null;
}

interface CandlestickChartProps {
  symbol: string;
  coordinatorSignal: CoordinatorSignalAPI | null;
  report: CoordinatorReportAPI | null;
}

type Timeframe = "M15" | "H1" | "H4" | "D1";

const TF_DAYS: Record<Timeframe, number> = {
  M15: 7,
  H1: 30,
  H4: 90,
  D1: 365,
};

const TIMEFRAMES: Timeframe[] = ["M15", "H1", "H4", "D1"];

export function CandlestickChart({ symbol, coordinatorSignal, report }: CandlestickChartProps) {
  const chartContainerRef = useRef<HTMLDivElement>(null);
  const chartRef = useRef<IChartApi | null>(null);
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  const seriesRef = useRef<any>(null);
  const [tooltip, setTooltip] = useState<TooltipData | null>(null);
  const [tooltipPosition, setTooltipPosition] = useState({ x: 0, y: 0 });
  const [activeTimeframe, setActiveTimeframe] = useState<Timeframe>("H1");

  const { bars, loading: ohlcvLoading } = useOhlcv(symbol, activeTimeframe, TF_DAYS[activeTimeframe]);

  // Build overlay values from real coordinator signal
  const action = toActionLabel(coordinatorSignal?.suggested_action ?? null);
  const conviction = coordinatorSignal?.conviction_score != null
    ? Math.round(coordinatorSignal.conviction_score * 100)
    : null;
  const posSize = coordinatorSignal?.position_size_pct ?? null;
  const slPct = coordinatorSignal?.sl_pct ?? null;
  const tpPct = coordinatorSignal?.tp_pct ?? null;
  const regime = coordinatorSignal?.regime ?? null;
  const topPick = report?.top_pick ?? null;

  // ── Chart init ───────────────────────────────────────────────────────────────
  useEffect(() => {
    if (!chartContainerRef.current) return;

    const chart = createChart(chartContainerRef.current, {
      layout: {
        background: { type: ColorType.Solid, color: "#FFFFFF" },
        textColor: "#6B7280",
      },
      grid: {
        vertLines: { color: "#F0F2F5" },
        horzLines: { color: "#F0F2F5" },
      },
      width: chartContainerRef.current.clientWidth,
      height: chartContainerRef.current.clientHeight,
      crosshair: {
        vertLine: { color: "#1F4AA8", width: 1, style: 2 },
        horzLine: { color: "#1F4AA8", width: 1, style: 2 },
      },
      rightPriceScale: { borderColor: "#E3E6EA" },
      timeScale: {
        borderColor: "#E3E6EA",
        timeVisible: true,
        secondsVisible: false,
      },
    });

    chartRef.current = chart;

    const candlestickSeries = chart.addSeries(CandlestickSeries, {
      upColor: "#0D9488",
      downColor: "#DC2626",
      borderUpColor: "#0D9488",
      borderDownColor: "#DC2626",
      wickUpColor: "#0D9488",
      wickDownColor: "#DC2626",
    });

    seriesRef.current = candlestickSeries;

    chart.subscribeCrosshairMove((param) => {
      if (!param.point || !param.time) {
        setTooltip(null);
        return;
      }

      const candleData = param.seriesData.get(candlestickSeries) as CandlestickData;
      if (candleData) {
        const date = new Date((param.time as number) * 1000);
        setTooltip({
          time: date.toLocaleString(),
          open: candleData.open.toFixed(5),
          high: candleData.high.toFixed(5),
          low: candleData.low.toFixed(5),
          close: candleData.close.toFixed(5),
          rsi: null,
          macd: null,
          bbPercent: null,
          ema200: null,
          atrRank: null,
        });
        setTooltipPosition({ x: param.point.x, y: param.point.y });
      }
    });

    const handleResize = () => {
      if (chartContainerRef.current && chartRef.current) {
        const { clientWidth, clientHeight } = chartContainerRef.current;
        if (clientWidth > 0 && clientHeight > 0) {
          chartRef.current.applyOptions({ width: clientWidth, height: clientHeight });
        }
      }
    };

    const resizeObserver = new ResizeObserver(handleResize);
    resizeObserver.observe(chartContainerRef.current);
    window.addEventListener("resize", handleResize);
    setTimeout(handleResize, 0);

    return () => {
      window.removeEventListener("resize", handleResize);
      resizeObserver.disconnect();
      chart.remove();
    };
  }, [symbol]);

  // ── Feed real OHLCV data ─────────────────────────────────────────────────────
  useEffect(() => {
    if (!seriesRef.current || bars.length === 0) return;

    const chartData: CandlestickData[] = bars.map((b) => ({
      time: Math.floor(new Date(b.timestamp_utc).getTime() / 1000) as Time,
      open: b.open,
      high: b.high,
      low: b.low,
      close: b.close,
    }));

    seriesRef.current.setData(chartData);
    chartRef.current?.timeScale().fitContent();
  }, [bars]);

  return (
    <div className="flex-1 bg-card rounded-md border border-border overflow-hidden flex flex-col h-full">
      {/* Overlay Strip */}
      <div className="h-10 bg-muted/50 border-b border-border flex items-center px-4 gap-6 shrink-0">
        {topPick && (
          <div className="flex items-center gap-2">
            <span className="text-[10px] text-muted-foreground uppercase tracking-wider">Top Pick</span>
            <span className="text-xs font-semibold">{topPick}</span>
          </div>
        )}
        <div className="flex items-center gap-2">
          <span className="text-[10px] text-muted-foreground uppercase tracking-wider">Action</span>
          <Badge
            className={
              action === "BUY"
                ? "bg-[var(--buy)] text-white text-[10px] px-1.5 h-5"
                : action === "SELL"
                ? "bg-[var(--sell)] text-white text-[10px] px-1.5 h-5"
                : "bg-muted text-muted-foreground text-[10px] px-1.5 h-5"
            }
          >
            {coordinatorSignal ? action : "—"}
          </Badge>
        </div>
        {conviction != null && (
          <div className="flex items-center gap-2">
            <span className="text-[10px] text-muted-foreground uppercase tracking-wider">Conviction</span>
            <span className="text-xs font-mono font-medium">{conviction}%</span>
          </div>
        )}
        {posSize != null && (
          <div className="flex items-center gap-2">
            <span className="text-[10px] text-muted-foreground uppercase tracking-wider">Pos Size</span>
            <span className="text-xs font-mono">{posSize.toFixed(1)}%</span>
          </div>
        )}
        {slPct != null && tpPct != null && (
          <div className="flex items-center gap-2">
            <span className="text-[10px] text-muted-foreground uppercase tracking-wider">SL/TP</span>
            <span className="text-xs font-mono">
              <span className="text-[var(--short)]">{slPct.toFixed(2)}%</span>
              {" / "}
              <span className="text-[var(--long)]">{tpPct.toFixed(2)}%</span>
            </span>
          </div>
        )}
        {regime && (
          <div className="flex items-center gap-2">
            <span className="text-[10px] text-muted-foreground uppercase tracking-wider">Regime</span>
            <Badge variant="secondary" className="text-[10px] px-1.5 h-5">
              {regime}
            </Badge>
          </div>
        )}
        <div className="flex items-center gap-0.5 ml-auto">
          {TIMEFRAMES.map((tf) => (
            <button
              key={tf}
              onClick={() => setActiveTimeframe(tf)}
              className={cn(
                "px-2 py-0.5 text-[10px] font-medium rounded transition-colors",
                activeTimeframe === tf
                  ? "bg-primary text-primary-foreground"
                  : "text-muted-foreground hover:text-foreground hover:bg-muted"
              )}
            >
              {tf}
            </button>
          ))}
          {ohlcvLoading && (
            <span className="text-[10px] text-muted-foreground ml-2">Loading…</span>
          )}
        </div>
      </div>

      {/* Chart Container */}
      <div className="flex-1 relative overflow-hidden">
        <div ref={chartContainerRef} className="absolute inset-0 w-full h-full" />

        {/* Tooltip */}
        {tooltip && (
          <div
            className="absolute z-10 bg-card border border-border rounded shadow-lg p-2 pointer-events-none text-xs"
            style={{
              left: Math.min(tooltipPosition.x + 10, (chartContainerRef.current?.clientWidth || 0) - 180),
              top: Math.max(tooltipPosition.y - 100, 10),
            }}
          >
            <div className="font-medium mb-1">{tooltip.time}</div>
            <div className="grid grid-cols-2 gap-x-3 gap-y-0.5 text-[10px]">
              <span className="text-muted-foreground">Open:</span>
              <span className="font-mono text-right">{tooltip.open}</span>
              <span className="text-muted-foreground">High:</span>
              <span className="font-mono text-right text-[var(--long)]">{tooltip.high}</span>
              <span className="text-muted-foreground">Low:</span>
              <span className="font-mono text-right text-[var(--short)]">{tooltip.low}</span>
              <span className="text-muted-foreground">Close:</span>
              <span className="font-mono text-right">{tooltip.close}</span>
            </div>
          </div>
        )}
      </div>
    </div>
  );
}
