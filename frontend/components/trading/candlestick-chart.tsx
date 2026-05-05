"use client";

import { useEffect, useRef, useState } from "react";
import { createChart, ColorType, IChartApi, CandlestickData, Time, CandlestickSeries } from "lightweight-charts";
import { Badge } from "@/components/ui/badge";

interface ChartOverlayData {
  topPick: string;
  action: "BUY" | "SELL" | "HOLD";
  conviction: number;
  positionSize: number;
  sl: string;
  tp: string;
  regime: string;
}

const overlayData: ChartOverlayData = {
  topPick: "EURUSD",
  action: "BUY",
  conviction: 78,
  positionSize: 2.5,
  sl: "1.0780",
  tp: "1.0920",
  regime: "Trending",
};

interface TooltipData {
  time: string;
  open: string;
  high: string;
  low: string;
  close: string;
  rsi: string;
  macd: string;
  bbPercent: string;
  ema200: boolean;
  atrRank: string;
}

// Generate mock candlestick data
function generateCandlestickData(): CandlestickData[] {
  const data: CandlestickData[] = [];
  let basePrice = 1.0800;
  const now = new Date();

  for (let i = 100; i >= 0; i--) {
    const date = new Date(now);
    date.setHours(date.getHours() - i);

    const volatility = 0.001 + Math.random() * 0.002;
    const trend = Math.sin(i / 20) * 0.0005;

    const open = basePrice + trend;
    const close = open + (Math.random() - 0.48) * volatility;
    const high = Math.max(open, close) + Math.random() * volatility * 0.5;
    const low = Math.min(open, close) - Math.random() * volatility * 0.5;

    data.push({
      time: Math.floor(date.getTime() / 1000) as Time,
      open: Number(open.toFixed(5)),
      high: Number(high.toFixed(5)),
      low: Number(low.toFixed(5)),
      close: Number(close.toFixed(5)),
    });

    basePrice = close;
  }

  return data;
}

interface CandlestickChartProps {
  symbol: string;
}

export function CandlestickChart({ symbol }: CandlestickChartProps) {
  const chartContainerRef = useRef<HTMLDivElement>(null);
  const chartRef = useRef<IChartApi | null>(null);
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  const seriesRef = useRef<any>(null);
  const [tooltip, setTooltip] = useState<TooltipData | null>(null);
  const [tooltipPosition, setTooltipPosition] = useState({ x: 0, y: 0 });

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
      rightPriceScale: {
        borderColor: "#E3E6EA",
      },
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
    const data = generateCandlestickData();
    candlestickSeries.setData(data);
    chart.timeScale().fitContent();

    // Handle crosshair move for tooltip
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
          rsi: (45 + Math.random() * 30).toFixed(1),
          macd: (Math.random() * 0.002 - 0.001).toFixed(5),
          bbPercent: (Math.random() * 100).toFixed(1),
          ema200: candleData.close > 1.082,
          atrRank: Math.random() > 0.5 ? "High" : "Normal",
        });
        setTooltipPosition({ x: param.point.x, y: param.point.y });
      }
    });

    const handleResize = () => {
      if (chartContainerRef.current && chartRef.current) {
        const { clientWidth, clientHeight } = chartContainerRef.current;
        if (clientWidth > 0 && clientHeight > 0) {
          chartRef.current.applyOptions({
            width: clientWidth,
            height: clientHeight,
          });
        }
      }
    };

    // Use ResizeObserver to handle container size changes (from splitters)
    const resizeObserver = new ResizeObserver(handleResize);
    resizeObserver.observe(chartContainerRef.current);

    window.addEventListener("resize", handleResize);

    // Initial resize after mount
    setTimeout(handleResize, 0);

    return () => {
      window.removeEventListener("resize", handleResize);
      resizeObserver.disconnect();
      chart.remove();
    };
  }, [symbol]);

  return (
    <div className="flex-1 bg-card rounded-md border border-border overflow-hidden flex flex-col h-full">
      {/* Overlay Strip */}
      <div className="h-10 bg-muted/50 border-b border-border flex items-center px-4 gap-6 shrink-0">
        <div className="flex items-center gap-2">
          <span className="text-[10px] text-muted-foreground uppercase tracking-wider">Top Pick</span>
          <span className="text-xs font-semibold">{overlayData.topPick}</span>
        </div>
        <div className="flex items-center gap-2">
          <span className="text-[10px] text-muted-foreground uppercase tracking-wider">Action</span>
          <Badge
            className={
              overlayData.action === "BUY"
                ? "bg-[var(--buy)] text-white text-[10px] px-1.5 h-5"
                : overlayData.action === "SELL"
                ? "bg-[var(--sell)] text-white text-[10px] px-1.5 h-5"
                : "bg-[var(--flat)] text-white text-[10px] px-1.5 h-5"
            }
          >
            {overlayData.action}
          </Badge>
        </div>
        <div className="flex items-center gap-2">
          <span className="text-[10px] text-muted-foreground uppercase tracking-wider">Conviction</span>
          <span className="text-xs font-mono font-medium">{overlayData.conviction}%</span>
        </div>
        <div className="flex items-center gap-2">
          <span className="text-[10px] text-muted-foreground uppercase tracking-wider">Pos Size</span>
          <span className="text-xs font-mono">{overlayData.positionSize}%</span>
        </div>
        <div className="flex items-center gap-2">
          <span className="text-[10px] text-muted-foreground uppercase tracking-wider">SL/TP</span>
          <span className="text-xs font-mono">
            <span className="text-[var(--short)]">{overlayData.sl}</span>
            {" / "}
            <span className="text-[var(--long)]">{overlayData.tp}</span>
          </span>
        </div>
        <div className="flex items-center gap-2">
          <span className="text-[10px] text-muted-foreground uppercase tracking-wider">Regime</span>
          <Badge variant="secondary" className="text-[10px] px-1.5 h-5">
            {overlayData.regime}
          </Badge>
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
            <div className="border-t border-border mt-1.5 pt-1.5 grid grid-cols-2 gap-x-3 gap-y-0.5 text-[10px]">
              <span className="text-muted-foreground">RSI:</span>
              <span className="font-mono text-right">{tooltip.rsi}</span>
              <span className="text-muted-foreground">MACD hist:</span>
              <span className="font-mono text-right">{tooltip.macd}</span>
              <span className="text-muted-foreground">BB%:</span>
              <span className="font-mono text-right">{tooltip.bbPercent}%</span>
              <span className="text-muted-foreground">EMA200:</span>
              <span className={`text-right ${tooltip.ema200 ? "text-[var(--long)]" : "text-[var(--short)]"}`}>
                {tooltip.ema200 ? "Above" : "Below"}
              </span>
              <span className="text-muted-foreground">ATR Rank:</span>
              <span className="text-right">{tooltip.atrRank}</span>
            </div>
          </div>
        )}
      </div>
    </div>
  );
}
