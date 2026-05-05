import type { Metadata } from "next";

import { ReportDocument } from "@/components/report/report-document";
import { reportHtmlBySymbol } from "@/lib/report-html";
import { normalizeReportSymbol, reportSymbols } from "@/lib/report-paths";

interface ReportPageProps {
  params: Promise<{
    symbol: string;
  }>;
}

export function generateStaticParams() {
  return reportSymbols.map((symbol) => ({ symbol }));
}

export async function generateMetadata({ params }: ReportPageProps): Promise<Metadata> {
  const { symbol } = await params;
  const reportSymbol = normalizeReportSymbol(symbol);

  return {
    title: `${reportSymbol} Deep Dive Report | FX-AlphaLab`,
    description: `${reportSymbol} autonomous market report from the FX-AlphaLab report template.`,
  };
}

export default async function ReportPage({ params }: ReportPageProps) {
  const { symbol } = await params;
  const reportSymbol = normalizeReportSymbol(symbol);

  return <ReportDocument html={reportHtmlBySymbol[reportSymbol]} symbol={reportSymbol} />;
}
