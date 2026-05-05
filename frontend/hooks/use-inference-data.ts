"use client";

import { useEffect, useRef, useState } from "react";
import {
  AgentSignalAPI,
  CoordinatorReportAPI,
  CoordinatorSignalAPI,
  fetchLatestReport,
  fetchSignals,
} from "@/lib/api";

export interface InferenceData {
  report: CoordinatorReportAPI | null;
  agentSignals: Map<string, AgentSignalAPI>;
  coordinatorSignals: Map<string, CoordinatorSignalAPI>;
  loading: boolean;
  error: string | null;
}

const POLL_INTERVAL_MS = 30_000;

export function useInferenceData(): InferenceData {
  const [report, setReport] = useState<CoordinatorReportAPI | null>(null);
  const [agentSignals, setAgentSignals] = useState<Map<string, AgentSignalAPI>>(new Map());
  const [coordinatorSignals, setCoordinatorSignals] = useState<Map<string, CoordinatorSignalAPI>>(new Map());
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const abortRef = useRef<AbortController | null>(null);

  async function load() {
    abortRef.current?.abort();
    abortRef.current = new AbortController();

    try {
      const rep = await fetchLatestReport();
      const signals = await fetchSignals(rep.date);

      setReport(rep);
      setAgentSignals(
        new Map(signals.agent_signals.map((s) => [s.pair, s]))
      );
      setCoordinatorSignals(
        new Map(signals.coordinator_signals.map((s) => [s.pair, s]))
      );
      setError(null);
    } catch (err: unknown) {
      if (err instanceof Error && err.name === "AbortError") return;
      setError(err instanceof Error ? err.message : "Failed to load inference data");
    } finally {
      setLoading(false);
    }
  }

  useEffect(() => {
    load();
    const timer = setInterval(load, POLL_INTERVAL_MS);
    return () => {
      clearInterval(timer);
      abortRef.current?.abort();
    };
  }, []);

  return { report, agentSignals, coordinatorSignals, loading, error };
}
