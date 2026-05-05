"use client";

import { useState } from "react";
import { TopBar } from "@/components/trading/top-bar";
import { LeftSidebar } from "@/components/trading/left-sidebar";
import { CandlestickChart } from "@/components/trading/candlestick-chart";
import { RightPanel } from "@/components/trading/right-panel";
import { BottomPanel } from "@/components/trading/bottom-panel";
import { Splitter } from "@/components/trading/splitter";
import { useResizableLayout } from "@/hooks/use-resizable-layout";

export default function TradingDashboard() {
  const [activeInstrument, setActiveInstrument] = useState("EURUSD");
  const [sidebarCollapsed, setSidebarCollapsed] = useState(false);
  const { sizes, handleMouseDown, resetLayout } = useResizableLayout();

  return (
    <div className="flex h-screen w-screen flex-col overflow-hidden bg-background">
      {/* Top Bar - Fixed Height */}
      <TopBar
        activeInstrument={activeInstrument}
        onInstrumentChange={setActiveInstrument}
        onResetLayout={resetLayout}
      />

      {/* Main Content Area */}
      <div className="flex flex-1 overflow-hidden">
        {/* Left Sidebar - Fixed Width */}
        <LeftSidebar
          collapsed={sidebarCollapsed}
          onToggle={() => setSidebarCollapsed(!sidebarCollapsed)}
          activeInstrument={activeInstrument}
          onInstrumentChange={setActiveInstrument}
        />

        {/* Center Area - Chart + Bottom Panel */}
        <div className="flex min-w-[300px] flex-1 flex-col gap-0 overflow-hidden p-2">
          {/* Candlestick Chart - must fill remaining space */}
          <CandlestickChart symbol={activeInstrument} />

          {/* Horizontal Splitter */}
          <Splitter
            orientation="horizontal"
            onMouseDown={(e) => handleMouseDown("bottom", e)}
          />

          {/* Bottom Panel - Positions */}
          <BottomPanel height={sizes.bottomPanel} />
        </div>

        {/* Right Splitter */}
        <Splitter
          orientation="vertical"
          onMouseDown={(e) => handleMouseDown("right", e)}
        />

        {/* Right Panel - Alpha Assistant */}
        <RightPanel symbol={activeInstrument} width={sizes.rightPanel} />
      </div>
    </div>
  );
}
