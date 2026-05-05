"use client";

import { useState } from "react";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Tabs, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { cn } from "@/lib/utils";
import {
  macroIndicators,
  calendarEvents,
  sentimentData,
  geoEvents,
  newsItems,
} from "@/lib/real-market-data";

interface RightPanelProps {
  symbol: string;
  width?: number;
}

function MacroDataWidget() {
  // Show key macro indicators relevant to FX
  const keyIndicators = macroIndicators.slice(0, 4);
  const getIndicatorColor = (change: number) => {
    if (change > 0) return "text-green-500";
    if (change < 0) return "text-red-500";
    return "text-muted-foreground";
  };

  return (
    <div className="p-3 border-b border-border">
      <h3 className="text-xs font-semibold uppercase tracking-wider text-muted-foreground mb-3">
        Macro Dashboard
      </h3>
      <div className="space-y-2">
        {keyIndicators.map((indicator) => (
          <div
            key={indicator.series_id}
            className="flex justify-between items-center text-xs"
          >
            <span className="text-muted-foreground">{indicator.name}</span>
            <div className="flex gap-2 items-center">
              <span className="font-mono font-semibold">{indicator.value}</span>
              <span
                className={cn(
                  "text-[10px] font-mono",
                  getIndicatorColor(indicator.value - indicator.previous)
                )}
              >
                {indicator.value - indicator.previous > 0 ? "+" : ""}
                {indicator.value - indicator.previous}
              </span>
            </div>
          </div>
        ))}
      </div>
    </div>
  );
}

function EconomicCalendarWidget() {
  const nextEvents = calendarEvents.slice(0, 3);

  const timeToEvent = (eventTime: string) => {
    const diff = new Date(eventTime).getTime() - Date.now();
    const hours = Math.floor(diff / 3600000);
    const minutes = Math.floor((diff % 3600000) / 60000);

    if (hours > 24) return `${Math.floor(hours / 24)}d`;
    if (hours > 0) return `${hours}h`;
    return `${minutes}m`;
  };

  return (
    <div className="p-3 border-b border-border">
      <h3 className="text-xs font-semibold uppercase tracking-wider text-muted-foreground mb-3">
        Upcoming Events (ForexFactory)
      </h3>
      <div className="space-y-2">
        {nextEvents.map((event) => (
          <div key={event.event_id} className="text-xs space-y-1">
            <div className="flex justify-between items-center">
              <span className="font-medium">{event.event_name}</span>
              <Badge
                variant="outline"
                className={cn(
                  "text-[10px] h-5",
                  event.impact === "High" && "border-red-500 text-red-500",
                  event.impact === "Medium" && "border-yellow-500 text-yellow-500",
                  event.impact === "Low" && "border-green-500 text-green-500"
                )}
              >
                {event.impact}
              </Badge>
            </div>
            <div className="flex justify-between text-muted-foreground text-[10px]">
              <span>
                {event.country} • {timeToEvent(event.time_utc)}
              </span>
              <span>
                {event.previous
                  ? `Prev: ${event.previous} • F: ${event.forecast}`
                  : `Forecast: ${event.forecast}`}
              </span>
            </div>
          </div>
        ))}
      </div>
    </div>
  );
}

function SentimentWidget() {
  const eurSentiment = sentimentData.find(
    (s) => s.symbol === "EURUSD" && s.source === "StockTwits"
  );
  const jpySentiment = sentimentData.find(
    (s) => s.symbol === "USDJPY" && s.source === "StockTwits"
  );

  const getBiasColor = (bias: string) => {
    if (bias === "bullish") return "bg-green-500/20 text-green-500";
    if (bias === "bearish") return "bg-red-500/20 text-red-500";
    return "bg-gray-500/20 text-gray-500";
  };

  return (
    <div className="p-3 border-b border-border">
      <h3 className="text-xs font-semibold uppercase tracking-wider text-muted-foreground mb-3">
        Sentiment (StockTwits)
      </h3>
      <div className="space-y-2">
        {[eurSentiment, jpySentiment]
          .filter(Boolean)
          .map((sentiment) => (
            <div
              key={sentiment!.symbol}
              className="flex justify-between items-center text-xs"
            >
              <div>
                <p className="font-medium">{sentiment!.symbol}</p>
                <p className="text-[10px] text-muted-foreground">
                  {sentiment!.active_users} active
                </p>
              </div>
              <div className="flex gap-2 items-center">
                <Badge
                  className={cn(
                    "text-[10px] capitalize h-5",
                    getBiasColor(sentiment!.bias)
                  )}
                >
                  {sentiment!.bias}
                </Badge>
                <span
                  className={cn(
                    "text-[10px] font-mono font-semibold",
                    sentiment!.volume_change_pct > 0
                      ? "text-green-500"
                      : "text-red-500"
                  )}
                >
                  {sentiment!.volume_change_pct > 0 ? "+" : ""}
                  {sentiment!.volume_change_pct}%
                </span>
              </div>
            </div>
          ))}
      </div>
    </div>
  );
}

function NewsWidget() {
  return (
    <div className="p-3 border-b border-border">
      <h3 className="text-xs font-semibold uppercase tracking-wider text-muted-foreground mb-3">
        Central Bank News
      </h3>
      <div className="space-y-2">
        {newsItems.slice(0, 2).map((news) => (
          <div key={news.article_id} className="text-xs space-y-1">
            <div className="flex justify-between items-start gap-2">
              <p className="font-medium line-clamp-2 flex-1">{news.title}</p>
              <Badge
                variant="outline"
                className={cn(
                  "text-[10px] h-5 shrink-0",
                  news.sentiment_tone === "Dovish" &&
                    "border-blue-500 text-blue-500",
                  news.sentiment_tone === "Hawkish" &&
                    "border-red-500 text-red-500",
                  news.sentiment_tone === "Neutral" &&
                    "border-gray-500 text-gray-500"
                )}
              >
                {news.sentiment_tone}
              </Badge>
            </div>
            <p className="text-[10px] text-muted-foreground">{news.source}</p>
          </div>
        ))}
      </div>
    </div>
  );
}

function GeopoliticalWidget() {
  return (
    <div className="p-3 border-b border-border">
      <h3 className="text-xs font-semibold uppercase tracking-wider text-muted-foreground mb-3">
        Geopolitical Events (GDELT)
      </h3>
      <div className="space-y-2">
        {geoEvents.slice(0, 2).map((event) => (
          <div key={event.event_id} className="text-xs space-y-1">
            <div className="flex justify-between items-start gap-2">
              <p className="font-medium line-clamp-2 flex-1 text-muted-foreground">
                {event.headline}
              </p>
              <Badge
                variant="outline"
                className={cn(
                  "text-[10px] h-5 shrink-0",
                  event.tone === "Positive" && "border-green-500 text-green-500",
                  event.tone === "Negative" && "border-red-500 text-red-500",
                  event.tone === "Neutral" && "border-gray-500 text-gray-500"
                )}
              >
                {event.tone}
              </Badge>
            </div>
            <div className="flex justify-between">
              <span className="text-[10px] text-muted-foreground">
                {event.countries.join(" / ")}
              </span>
              <span className="text-[10px] text-primary font-semibold">
                Relevance: {event.relevance_to_fx}%
              </span>
            </div>
          </div>
        ))}
      </div>
    </div>
  );
}

export function RightPanel({ symbol, width = 380 }: RightPanelProps) {
  return (
    <aside
      className="bg-card border-l border-border flex flex-col shrink-0 overflow-hidden shadow-[var(--card-shadow)]"
      style={{ width: `${width}px` }}
    >
      {/* Header */}
      <div className="p-3 border-b border-border">
        <h2 className="text-sm font-semibold">Market Intelligence</h2>
        <p className="text-[10px] text-muted-foreground mt-0.5">
          Real-time data from collectors
        </p>
      </div>

      {/* Scrollable Content */}
      <div className="flex-1 overflow-auto">
        <MacroDataWidget />
        <EconomicCalendarWidget />
        <SentimentWidget />
        <NewsWidget />
        <GeopoliticalWidget />
      </div>

      {/* Footer Info */}
      <div className="p-3 border-t border-border bg-muted/50">
        <p className="text-[9px] text-muted-foreground">
          Data sources: FRED (Fed), ECB, ForexFactory, StockTwits, GDELT
        </p>
        <p className="text-[9px] text-muted-foreground mt-1">
          Agents not yet active (W7+)
        </p>
      </div>
    </aside>
  );
}
