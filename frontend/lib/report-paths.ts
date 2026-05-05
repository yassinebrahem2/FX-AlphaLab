export const reportSymbols = ["EURUSD", "GBPUSD", "USDJPY"] as const;

export type ReportSymbol = (typeof reportSymbols)[number];

export function normalizeReportSymbol(symbol: string): ReportSymbol {
  const normalized = symbol.toUpperCase();

  return reportSymbols.includes(normalized as ReportSymbol) ? (normalized as ReportSymbol) : "EURUSD";
}

export function getReportPath(symbol: string): string {
  return `/reports/${normalizeReportSymbol(symbol)}`;
}
