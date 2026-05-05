"use client";

import { useState } from "react";
import { TopBar } from "@/components/trading/top-bar";
import { LeftSidebar } from "@/components/trading/left-sidebar";
import { CandlestickChart } from "@/components/trading/candlestick-chart";
import { RightPanel } from "@/components/trading/right-panel";
import { BottomPanel } from "@/components/trading/bottom-panel";
import { Splitter } from "@/components/trading/splitter";
import { useResizableLayout } from "@/hooks/use-resizable-layout";
import { useInferenceData } from "@/hooks/use-inference-data";

export default function TradingDashboard() {
  const [activeInstrument, setActiveInstrument] = useState("EURUSD");
  const [sidebarCollapsed, setSidebarCollapsed] = useState(false);
  const { sizes, handleMouseDown, resetLayout } = useResizableLayout();
  const inference = useInferenceData();

  return (
    <div className="h-screen w-screen flex flex-col overflow-hidden bg-background">
      <TopBar
        activeInstrument={activeInstrument}
        onInstrumentChange={setActiveInstrument}
        onResetLayout={resetLayout}
        report={inference.report}
      />

      <div className="flex-1 flex overflow-hidden">
        <LeftSidebar
          collapsed={sidebarCollapsed}
          onToggle={() => setSidebarCollapsed(!sidebarCollapsed)}
          activeInstrument={activeInstrument}
          onInstrumentChange={setActiveInstrument}
          width={sidebarCollapsed ? 40 : sizes.leftSidebar}
          coordinatorSignals={inference.coordinatorSignals}
          agentSignals={inference.agentSignals}
        />

        {!sidebarCollapsed && (
          <Splitter
            orientation="vertical"
            onMouseDown={(e) => handleMouseDown("left", e)}
          />
        )}

        <div className="flex-1 flex flex-col overflow-hidden p-2 gap-0 min-w-[300px]">
          <CandlestickChart
            symbol={activeInstrument}
            coordinatorSignal={inference.coordinatorSignals.get(activeInstrument) ?? null}
            report={inference.report}
          />

          <Splitter
            orientation="horizontal"
            onMouseDown={(e) => handleMouseDown("bottom", e)}
          />

          <BottomPanel height={sizes.bottomPanel} />
        </div>

        <Splitter
          orientation="vertical"
          onMouseDown={(e) => handleMouseDown("right", e)}
        />

        <RightPanel
          symbol={activeInstrument}
          width={sizes.rightPanel}
          report={inference.report}
          coordinatorSignals={inference.coordinatorSignals}
          agentSignals={inference.agentSignals}
        />
      </div>
    </div>
  );
}
