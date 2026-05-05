"use client";

import { useState, useEffect, useCallback, useRef } from "react";

const STORAGE_KEY = "fx-alphalab-layout";

export interface LayoutSizes {
  leftSidebar: number;
  rightPanel: number;
  bottomPanel: number;
}

const DEFAULT_SIZES: LayoutSizes = {
  leftSidebar: 300,
  rightPanel: 360,
  bottomPanel: 190,
};

const MIN_SIZES: LayoutSizes = {
  leftSidebar: 280,
  rightPanel: 320,
  bottomPanel: 160,
};

const MAX_SIZES: LayoutSizes = {
  leftSidebar: 320,
  rightPanel: 500,
  bottomPanel: 400,
};

export function useResizableLayout() {
  const [sizes, setSizes] = useState<LayoutSizes>(DEFAULT_SIZES);
  const [isHydrated, setIsHydrated] = useState(false);
  const isDraggingRef = useRef<"left" | "right" | "bottom" | null>(null);
  const startPosRef = useRef({ x: 0, y: 0 });
  const startSizeRef = useRef(0);

  // Load from localStorage on mount
  useEffect(() => {
    try {
      const stored = localStorage.getItem(STORAGE_KEY);
      if (stored) {
        const parsed = JSON.parse(stored) as LayoutSizes;
        setSizes({
          leftSidebar: Math.max(MIN_SIZES.leftSidebar, Math.min(MAX_SIZES.leftSidebar, parsed.leftSidebar || DEFAULT_SIZES.leftSidebar)),
          rightPanel: Math.max(MIN_SIZES.rightPanel, Math.min(MAX_SIZES.rightPanel, parsed.rightPanel || DEFAULT_SIZES.rightPanel)),
          bottomPanel: Math.max(MIN_SIZES.bottomPanel, Math.min(MAX_SIZES.bottomPanel, parsed.bottomPanel || DEFAULT_SIZES.bottomPanel)),
        });
      }
    } catch {
      // Ignore localStorage errors
    }
    setIsHydrated(true);
  }, []);

  // Save to localStorage when sizes change
  useEffect(() => {
    if (isHydrated) {
      try {
        localStorage.setItem(STORAGE_KEY, JSON.stringify(sizes));
      } catch {
        // Ignore localStorage errors
      }
    }
  }, [sizes, isHydrated]);

  const handleMouseDown = useCallback(
    (panel: "left" | "right" | "bottom", e: React.MouseEvent) => {
      e.preventDefault();
      isDraggingRef.current = panel;
      startPosRef.current = { x: e.clientX, y: e.clientY };

      if (panel === "left") {
        startSizeRef.current = sizes.leftSidebar;
      } else if (panel === "right") {
        startSizeRef.current = sizes.rightPanel;
      } else {
        startSizeRef.current = sizes.bottomPanel;
      }

      document.body.style.cursor = panel === "bottom" ? "row-resize" : "col-resize";
      document.body.style.userSelect = "none";
    },
    [sizes]
  );

  const handleMouseMove = useCallback((e: MouseEvent) => {
    if (!isDraggingRef.current) return;

    const panel = isDraggingRef.current;

    if (panel === "left") {
      const delta = e.clientX - startPosRef.current.x;
      const newSize = Math.max(
        MIN_SIZES.leftSidebar,
        Math.min(MAX_SIZES.leftSidebar, startSizeRef.current + delta)
      );
      setSizes((prev) => ({ ...prev, leftSidebar: newSize }));
    } else if (panel === "right") {
      const delta = startPosRef.current.x - e.clientX;
      const newSize = Math.max(
        MIN_SIZES.rightPanel,
        Math.min(MAX_SIZES.rightPanel, startSizeRef.current + delta)
      );
      setSizes((prev) => ({ ...prev, rightPanel: newSize }));
    } else if (panel === "bottom") {
      const delta = startPosRef.current.y - e.clientY;
      const newSize = Math.max(
        MIN_SIZES.bottomPanel,
        Math.min(MAX_SIZES.bottomPanel, startSizeRef.current + delta)
      );
      setSizes((prev) => ({ ...prev, bottomPanel: newSize }));
    }
  }, []);

  const handleMouseUp = useCallback(() => {
    isDraggingRef.current = null;
    document.body.style.cursor = "";
    document.body.style.userSelect = "";
  }, []);

  // Attach global mouse listeners
  useEffect(() => {
    document.addEventListener("mousemove", handleMouseMove);
    document.addEventListener("mouseup", handleMouseUp);
    return () => {
      document.removeEventListener("mousemove", handleMouseMove);
      document.removeEventListener("mouseup", handleMouseUp);
    };
  }, [handleMouseMove, handleMouseUp]);

  const resetLayout = useCallback(() => {
    setSizes(DEFAULT_SIZES);
    try {
      localStorage.removeItem(STORAGE_KEY);
    } catch {
      // Ignore
    }
  }, []);

  return {
    sizes: isHydrated ? sizes : DEFAULT_SIZES,
    handleMouseDown,
    resetLayout,
    isHydrated,
  };
}
