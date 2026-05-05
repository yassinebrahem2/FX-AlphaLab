"use client";

import { useEffect, useRef, useState } from "react";
import { fetchOHLCV, OHLCVBarAPI } from "@/lib/api";

export interface OHLCVState {
  bars: OHLCVBarAPI[];
  loading: boolean;
  error: string | null;
}

export function useOhlcv(instrument: string, tf = "H1", days = 30): OHLCVState {
  const [bars, setBars] = useState<OHLCVBarAPI[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const abortRef = useRef<AbortController | null>(null);

  useEffect(() => {
    abortRef.current?.abort();
    abortRef.current = new AbortController();
    setLoading(true);

    fetchOHLCV(instrument, tf, days)
      .then((data) => {
        setBars(data);
        setError(null);
      })
      .catch((err: unknown) => {
        if (err instanceof Error && err.name === "AbortError") return;
        setError(err instanceof Error ? err.message : "Failed to load OHLCV");
      })
      .finally(() => setLoading(false));

    return () => abortRef.current?.abort();
  }, [instrument, tf, days]);

  return { bars, loading, error };
}
