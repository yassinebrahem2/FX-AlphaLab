"use client";

import { cn } from "@/lib/utils";
import { GripVertical, GripHorizontal } from "lucide-react";

interface SplitterProps {
  orientation: "vertical" | "horizontal";
  onMouseDown: (e: React.MouseEvent) => void;
  className?: string;
}

export function Splitter({ orientation, onMouseDown, className }: SplitterProps) {
  const isVertical = orientation === "vertical";

  return (
    <div
      className={cn(
        "group flex items-center justify-center shrink-0 transition-colors",
        isVertical
          ? "w-2 cursor-col-resize hover:bg-primary/10 active:bg-primary/20"
          : "h-2 cursor-row-resize hover:bg-primary/10 active:bg-primary/20",
        className
      )}
      onMouseDown={onMouseDown}
    >
      <div
        className={cn(
          "flex items-center justify-center rounded transition-all",
          isVertical
            ? "w-4 h-8 opacity-0 group-hover:opacity-100"
            : "h-4 w-8 opacity-0 group-hover:opacity-100"
        )}
      >
        {isVertical ? (
          <GripVertical className="h-4 w-4 text-muted-foreground" />
        ) : (
          <GripHorizontal className="h-4 w-4 text-muted-foreground" />
        )}
      </div>
    </div>
  );
}
