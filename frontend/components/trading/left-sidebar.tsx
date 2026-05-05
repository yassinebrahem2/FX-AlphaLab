"use client";

import { ChevronLeft, ChevronRight } from "lucide-react";

import { mockAgentReport } from "@/lib/agent-data";

import {
  AgentPulsePanel,
  WatchlistPanel,
  type AgentPulse,
  type WatchlistItem,
} from "@/components/trading/sidebar-panels";

const watchlist: WatchlistItem[] = [
  { symbol: "EURUSD", direction: "LONG", confidence: "3/3", bid: "1.0842", ask: "1.0844" },
  { symbol: "GBPUSD", direction: "SHORT", confidence: "2/3", bid: "1.2651", ask: "1.2653" },
  { symbol: "USDJPY", direction: "LONG", confidence: "2/3", bid: "154.82", ask: "154.84" },
  { symbol: "USDCHF", direction: "FLAT", confidence: "1/3", bid: "0.8842", ask: "0.8844" },
  { symbol: "AUDUSD", direction: "SHORT", confidence: "3/3", bid: "0.6512", ask: "0.6514" },
  { symbol: "NZDUSD", direction: "LONG", confidence: "2/3", bid: "0.5982", ask: "0.5984" },
  { symbol: "USDCAD", direction: "FLAT", confidence: "1/3", bid: "1.3652", ask: "1.3654" },
];

function formatReportFreshness(timestamp: string): string {
  const diffMs = Date.now() - new Date(timestamp).getTime();
  const minutes = Math.max(1, Math.floor(diffMs / 60000));

  if (minutes < 60) return `${minutes}m ago`;

  const hours = Math.floor(minutes / 60);
  if (hours < 24) return `${hours}h ago`;

  return `${Math.floor(hours / 24)}d ago`;
}

function toConfidenceUnit(value: number): string {
  const normalized = value > 1 ? value / 100 : value;
  return normalized.toFixed(2);
}

function toMacroDirection(value: string): string {
  if (value === "LONG") return "up";
  if (value === "SHORT") return "down";
  if (value === "NEUTRAL") return "neutral";
  return value.toLowerCase();
}

function toTechnicalDirection(value: string): string {
  if (value === "LONG") return "bullish";
  if (value === "SHORT") return "bearish";
  if (value === "FLAT") return "flat";
  return value.toLowerCase();
}

const report = mockAgentReport;

const activeCoordinatorPair = report.coordinator.per_pair.find(
  (pair) => pair.symbol === "EURUSD"
) ?? report.coordinator.per_pair[0];
const technicalReport = report.technical.find((item) => item.symbol === "EURUSD") ?? report.technical[0];
const macroReport = report.macro.find((item) => item.symbol === "EURUSD") ?? report.macro[0];
const geopoliticalReport = report.geopolitical.find((item) => item.symbol === "EURUSD") ?? report.geopolitical[0];
const sentimentReport = report.sentiment[0];

const agentPulse: AgentPulse[] = [
  {
    name: "Technical",
    freshness: formatReportFreshness(report.timestamp),
    primaryOutput: toTechnicalDirection(technicalReport.direction),
    secondaryOutput: `conf ${toConfidenceUnit(technicalReport.confidence)}`,
    insight: `${technicalReport.timeframe_votes.filter((vote) => vote.direction === "LONG").length}/3 aligned | ${technicalReport.volatility_regime.toLowerCase()} vol`,
  },
  {
    name: "Macro",
    freshness: formatReportFreshness(report.timestamp),
    primaryOutput: toMacroDirection(macroReport.module_c_direction),
    secondaryOutput: `conf ${toConfidenceUnit(macroReport.macro_confidence)}`,
    insight: `${macroReport.dominant_driver} leads`,
  },
  {
    name: "Geo",
    freshness: formatReportFreshness(report.timestamp),
    primaryOutput: geopoliticalReport.risk_regime.toLowerCase(),
    secondaryOutput: `score ${geopoliticalReport.bilateral_risk_score.toFixed(2)}`,
    insight: `base driver: ${geopoliticalReport.base_dominant_driver}`,
  },
  {
    name: "Sentiment",
    freshness: formatReportFreshness(report.timestamp),
    primaryOutput: sentimentReport.usdjpy_stocktwits_active ? "active" : "inactive",
    secondaryOutput: sentimentReport.composite_stress_flag ? "stress on" : "stress off",
    insight: sentimentReport.stress_sources.length
      ? sentimentReport.stress_sources.join(", ")
      : "no stress trigger",
  },
];

interface LeftSidebarProps {
  collapsed: boolean;
  onToggle: () => void;
  activeInstrument: string;
  onInstrumentChange: (symbol: string) => void;
}

export function LeftSidebar({ collapsed, onToggle, activeInstrument, onInstrumentChange }: LeftSidebarProps) {
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
      <aside className="h-[calc(100vh-64px)] w-[300px] min-w-[280px] max-w-[320px] shrink-0 overflow-hidden border-r border-border bg-card shadow-[var(--card-shadow)]">
      {/* Watchlist Section */}
      <div className="flex min-h-0 flex-1 flex-col overflow-hidden">
        <WatchlistPanel
          items={watchlist}
          activeInstrument={activeInstrument}
          onInstrumentChange={onInstrumentChange}
          onToggle={onToggle}
        />

        <AgentPulsePanel items={agentPulse} />
      </div>
    </aside>
  );
}
