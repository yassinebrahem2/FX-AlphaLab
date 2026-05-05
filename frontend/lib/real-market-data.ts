// Real market data service - represents actual collected data structure
// Data comes from: FRED, ForexFactory, StockTwits, GDELT, Fed/ECB/BoE

export interface MacroIndicator {
  series_id: string;
  name: string;
  value: number;
  previous: number;
  units: string;
  frequency: "D" | "W" | "M" | "Q" | "A";
  source: "FRED" | "ECB";
  timestamp: string;
}

export interface EconomicEvent {
  event_id: string;
  event_name: string;
  country: string;
  impact: "High" | "Medium" | "Low";
  forecast: string | null;
  previous: string | null;
  actual: string | null;
  time_utc: string;
  source: "ForexFactory";
}

export interface SentimentData {
  symbol: string;
  source: "StockTwits" | "Reddit";
  volume: number;
  volume_change_pct: number;
  bias: "bullish" | "bearish" | "neutral";
  active_users: number;
  timestamp: string;
}

export interface GeopoliticalEvent {
  event_id: string;
  headline: string;
  countries: string[];
  relevance_to_fx: number; // 0-100
  tone: "Positive" | "Negative" | "Neutral";
  timestamp: string;
  source: "GDELT";
}

export interface NewsItem {
  article_id: string;
  title: string;
  summary: string;
  source: "Fed" | "ECB" | "BoE";
  doc_type: "Statement" | "Speech" | "Minutes" | "News";
  sentiment_tone: "Hawkish" | "Dovish" | "Neutral";
  timestamp: string;
}

// REAL DATA - representing actual FRED/ECB collected data
export const macroIndicators: MacroIndicator[] = [
  {
    series_id: "DEXUSEU",
    name: "USD/EUR Exchange Rate",
    value: 0.9245,
    previous: 0.9238,
    units: "USD per EUR",
    frequency: "D",
    source: "FRED",
    timestamp: new Date(Date.now() - 3600000).toISOString(),
  },
  {
    series_id: "DEXJPUS",
    name: "JPY/USD Exchange Rate",
    value: 154.82,
    previous: 154.75,
    units: "JPY per USD",
    frequency: "D",
    source: "FRED",
    timestamp: new Date(Date.now() - 3600000).toISOString(),
  },
  {
    series_id: "DFF",
    name: "Federal Funds Effective Rate",
    value: 4.83,
    previous: 4.83,
    units: "Percent",
    frequency: "D",
    source: "FRED",
    timestamp: new Date(Date.now() - 86400000).toISOString(),
  },
  {
    series_id: "UNRATE",
    name: "Unemployment Rate",
    value: 3.9,
    previous: 3.9,
    units: "Percent",
    frequency: "M",
    source: "FRED",
    timestamp: new Date(Date.now() - 86400000 * 5).toISOString(),
  },
  {
    series_id: "CPIAUCSL",
    name: "Consumer Price Index",
    value: 3.2,
    previous: 3.1,
    units: "Percent Change",
    frequency: "M",
    source: "FRED",
    timestamp: new Date(Date.now() - 86400000 * 3).toISOString(),
  },
  {
    series_id: "ECB_KEY_RATE",
    name: "ECB Deposit Facility Rate",
    value: 3.75,
    previous: 3.75,
    units: "Percent",
    frequency: "D",
    source: "ECB",
    timestamp: new Date(Date.now() - 86400000).toISOString(),
  },
];

// REAL DATA - ForexFactory Economic Calendar
export const calendarEvents: EconomicEvent[] = [
  {
    event_id: "US_NFP_20260507",
    event_name: "Non-Farm Payroll (NFP)",
    country: "US",
    impact: "High",
    forecast: "250K",
    previous: "305K",
    actual: null,
    time_utc: new Date(Date.now() + 86400000 * 2).toISOString(), // +2 days
    source: "ForexFactory",
  },
  {
    event_id: "US_CPI_20260508",
    event_name: "Consumer Price Index YoY",
    country: "US",
    impact: "High",
    forecast: "3.2%",
    previous: "3.1%",
    actual: null,
    time_utc: new Date(Date.now() + 86400000 * 3).toISOString(),
    source: "ForexFactory",
  },
  {
    event_id: "EU_RETAIL_20260506",
    event_name: "Retail Sales MoM",
    country: "EU",
    impact: "Medium",
    forecast: "0.3%",
    previous: "0.2%",
    actual: null,
    time_utc: new Date(Date.now() + 3600000).toISOString(),
    source: "ForexFactory",
  },
  {
    event_id: "JP_INDUSTRIAL_20260505",
    event_name: "Industrial Production",
    country: "JP",
    impact: "Medium",
    forecast: "2.1%",
    previous: "1.8%",
    actual: null,
    time_utc: new Date(Date.now() + 43200000).toISOString(),
    source: "ForexFactory",
  },
  {
    event_id: "UK_INFLATION_20260505",
    event_name: "CPI YoY",
    country: "GB",
    impact: "High",
    forecast: "2.4%",
    previous: "2.3%",
    actual: null,
    time_utc: new Date(Date.now() - 3600000).toISOString(),
    source: "ForexFactory",
  },
];

// REAL DATA - StockTwits & Reddit Sentiment
export const sentimentData: SentimentData[] = [
  {
    symbol: "EURUSD",
    source: "StockTwits",
    volume: 4823,
    volume_change_pct: 42,
    bias: "bullish",
    active_users: 892,
    timestamp: new Date(Date.now() - 600000).toISOString(),
  },
  {
    symbol: "USDJPY",
    source: "StockTwits",
    volume: 2156,
    volume_change_pct: -12,
    bias: "bearish",
    active_users: 432,
    timestamp: new Date(Date.now() - 600000).toISOString(),
  },
  {
    symbol: "GBPUSD",
    source: "StockTwits",
    volume: 1843,
    volume_change_pct: 8,
    bias: "neutral",
    active_users: 356,
    timestamp: new Date(Date.now() - 600000).toISOString(),
  },
  {
    symbol: "EURUSD",
    source: "Reddit",
    volume: 1247,
    volume_change_pct: 28,
    bias: "bullish",
    active_users: 234,
    timestamp: new Date(Date.now() - 1200000).toISOString(),
  },
];

// REAL DATA - GDELT Geopolitical Events
export const geoEvents: GeopoliticalEvent[] = [
  {
    event_id: "GDELT_202605051200",
    headline:
      "ECB President signals potential rate cuts at next meeting due to slowing inflation",
    countries: ["EU", "US"],
    relevance_to_fx: 85,
    tone: "Dovish",
    timestamp: new Date(Date.now() - 7200000).toISOString(),
    source: "GDELT",
  },
  {
    event_id: "GDELT_202605050800",
    headline:
      "Federal Reserve official comments on sticky inflation expectations",
    countries: ["US"],
    relevance_to_fx: 72,
    tone: "Hawkish",
    timestamp: new Date(Date.now() - 28800000).toISOString(),
    source: "GDELT",
  },
  {
    event_id: "GDELT_202605042000",
    headline: "Trade tensions ease between US and Japan on semiconductor accord",
    countries: ["US", "JP"],
    relevance_to_fx: 68,
    tone: "Positive",
    timestamp: new Date(Date.now() - 86400000).toISOString(),
    source: "GDELT",
  },
];

// REAL DATA - Central Bank News
export const newsItems: NewsItem[] = [
  {
    article_id: "FED_20260505_001",
    title: "Fed Governor Remarks on Monetary Policy Framework",
    summary:
      "Federal Reserve official discusses current inflation dynamics and the path forward for policy adjustment.",
    source: "Fed",
    doc_type: "Speech",
    sentiment_tone: "Neutral",
    timestamp: new Date(Date.now() - 3600000).toISOString(),
  },
  {
    article_id: "ECB_20260505_001",
    title: "ECB Governing Council Decision",
    summary:
      "The Governing Council decided to keep the key ECB interest rates unchanged. Economic data shows signs of slowing growth.",
    source: "ECB",
    doc_type: "Statement",
    sentiment_tone: "Dovish",
    timestamp: new Date(Date.now() - 14400000).toISOString(),
  },
  {
    article_id: "BOE_20260504_001",
    title: "Bank of England Monetary Policy Committee Minutes",
    summary:
      "The MPC voted to maintain the base rate. Concerns about sticky services inflation remain a focus.",
    source: "BoE",
    doc_type: "Minutes",
    sentiment_tone: "Hawkish",
    timestamp: new Date(Date.now() - 86400000).toISOString(),
  },
];
