"use client";

import Image from "next/image";
import { Bell, Settings, User, RotateCcw } from "lucide-react";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuTrigger,
  DropdownMenuSeparator,
} from "@/components/ui/dropdown-menu";
import { cn } from "@/lib/utils";

const instruments = [
  { symbol: "EURUSD", active: true },
  { symbol: "GBPUSD", active: false },
  { symbol: "USDJPY", active: false },
  { symbol: "USDCHF", active: false },
  { symbol: "AUDUSD", active: false },
];

interface TopBarProps {
  activeInstrument: string;
  onInstrumentChange: (symbol: string) => void;
  onResetLayout?: () => void;
}

export function TopBar({ activeInstrument, onInstrumentChange, onResetLayout }: TopBarProps) {
  return (
    <header className="flex h-16 shrink-0 items-center justify-between border-b border-border bg-card px-4 shadow-[var(--card-shadow)]">
      {/* Left Section */}
      <div className="flex items-center gap-4">
        <div className="flex items-center gap-2">
          <div className="flex h-8 w-[52px] items-center justify-center overflow-hidden rounded-sm">
            <Image src="/fx-mark1.png" alt="FX AlphaLab logo" width={52} height={32} className="h-auto w-full" />
          </div>
          <span className="font-semibold text-foreground">AlphaLab</span>
        </div>
        <Badge variant="default" className="bg-[var(--long)] text-white text-[10px] px-1.5 py-0.5 h-5">
          LIVE
        </Badge>
        <span className="text-xs text-muted-foreground">
          Last updated: <span className="font-mono">14:32:18</span>
        </span>
        <Badge variant="secondary" className="bg-amber-100 text-amber-700 text-[10px] px-1.5 py-0.5 h-5 border-amber-200">
          Stale Input
        </Badge>
      </div>

      {/* Center - Instrument Tabs */}
      <nav className="flex items-center gap-1">
        {instruments.map((inst) => (
          <button
            key={inst.symbol}
            onClick={() => onInstrumentChange(inst.symbol)}
            className={cn(
              "px-3 py-2 text-sm font-medium transition-colors relative",
              activeInstrument === inst.symbol
                ? "text-primary"
                : "text-muted-foreground hover:text-foreground"
            )}
          >
            {inst.symbol}
            {activeInstrument === inst.symbol && (
              <div className="absolute bottom-0 left-0 right-0 h-0.5 bg-primary" />
            )}
          </button>
        ))}
      </nav>

      {/* Right Section */}
      <div className="flex items-center gap-4">
        <div className="flex items-center gap-6 text-sm">
          <div className="flex flex-col items-end">
            <span className="text-[10px] text-muted-foreground uppercase tracking-wider">Equity</span>
            <span className="font-mono font-medium text-[var(--profit)]">$124,582.40</span>
          </div>
          <div className="flex flex-col items-end">
            <span className="text-[10px] text-muted-foreground uppercase tracking-wider">Balance</span>
            <span className="font-mono font-medium">$120,000.00</span>
          </div>
          <div className="flex flex-col items-end">
            <span className="text-[10px] text-muted-foreground uppercase tracking-wider">Margin</span>
            <span className="font-mono font-medium">$8,420.00</span>
          </div>
        </div>
        <div className="h-8 w-px bg-border" />
        <div className="flex items-center gap-1">
          <Button variant="ghost" size="icon" className="h-8 w-8">
            <Bell className="h-4 w-4" />
          </Button>
          <DropdownMenu>
            <DropdownMenuTrigger asChild>
              <Button variant="ghost" size="icon" className="h-8 w-8">
                <Settings className="h-4 w-4" />
              </Button>
            </DropdownMenuTrigger>
            <DropdownMenuContent align="end" className="w-48">
              <DropdownMenuItem>
                <Settings className="h-3.5 w-3.5 mr-2" />
                Preferences
              </DropdownMenuItem>
              <DropdownMenuSeparator />
              <DropdownMenuItem onClick={onResetLayout}>
                <RotateCcw className="h-3.5 w-3.5 mr-2" />
                Reset Layout
              </DropdownMenuItem>
            </DropdownMenuContent>
          </DropdownMenu>
          <Button variant="ghost" size="icon" className="h-8 w-8">
            <User className="h-4 w-4" />
          </Button>
        </div>
        <Button size="sm" className="bg-primary text-primary-foreground hover:bg-primary/90 h-8">
          Deposit
        </Button>
      </div>
    </header>
  );
}
