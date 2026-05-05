"use client";

import { useEffect, useRef } from "react";

import type { ReportHtmlSymbol } from "@/lib/report-html";

declare global {
  interface Window {
    Plotly?: unknown;
  }
}

interface ReportDocumentProps {
  html: string;
  symbol: ReportHtmlSymbol;
}

function loadScript(src: string): Promise<void> {
  return new Promise((resolve, reject) => {
    const existing = document.querySelector<HTMLScriptElement>(`script[src="${src}"]`);

    if (existing) {
      if (existing.dataset.loaded === "true") {
        resolve();
        return;
      }

      existing.addEventListener("load", () => resolve(), { once: true });
      existing.addEventListener("error", () => reject(new Error(`Failed to load ${src}`)), {
        once: true,
      });
      return;
    }

    const script = document.createElement("script");
    script.src = src;
    script.async = true;
    script.dataset.loaded = "false";
    script.onload = () => {
      script.dataset.loaded = "true";
      resolve();
    };
    script.onerror = () => reject(new Error(`Failed to load ${src}`));
    document.head.appendChild(script);
  });
}

function attachReportInteractions(container: HTMLElement) {
  const links = Array.from(container.querySelectorAll<HTMLAnchorElement>("[data-section-link]"));
  const sections = links
    .map((link) => container.querySelector<HTMLElement>(link.getAttribute("href") ?? ""))
    .filter(Boolean);

  const setActive = () => {
    let active = sections[0]?.id ?? "";

    for (const section of sections) {
      const rect = section.getBoundingClientRect();
      if (rect.top < 170) active = section.id;
    }

    links.forEach((link) => {
      link.classList.toggle("active", link.getAttribute("href") === `#${active}`);
    });
  };

  const toggleButtons = Array.from(container.querySelectorAll<HTMLButtonElement>("[data-toggle-expert]"));
  const onToggleExpert = () => {
    document.body.classList.toggle("show-expert");
    const appendix = container.querySelector<HTMLElement>("#expert-appendix");

    if (document.body.classList.contains("show-expert") && appendix) {
      appendix.scrollIntoView({ behavior: "smooth", block: "start" });
    }
  };

  const selects = Array.from(container.querySelectorAll<HTMLSelectElement>(".pair-select"));
  const onSelectReport = (event: Event) => {
    const target = event.target as HTMLSelectElement;
    window.location.href = `/reports/${target.value}`;
  };

  toggleButtons.forEach((button) => button.addEventListener("click", onToggleExpert));
  selects.forEach((select) => select.addEventListener("change", onSelectReport));
  window.addEventListener("scroll", setActive, { passive: true });
  setActive();

  return () => {
    toggleButtons.forEach((button) => button.removeEventListener("click", onToggleExpert));
    selects.forEach((select) => select.removeEventListener("change", onSelectReport));
    window.removeEventListener("scroll", setActive);
    document.body.classList.remove("show-expert");
  };
}

function runInlineScripts(container: HTMLElement) {
  const scripts = Array.from(container.querySelectorAll<HTMLScriptElement>("script"));

  scripts.forEach((script) => {
    const executable = document.createElement("script");
    executable.text = script.textContent ?? "";
    document.body.appendChild(executable);
    document.body.removeChild(executable);
  });
}

export function ReportDocument({ html, symbol }: ReportDocumentProps) {
  const containerRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    const container = containerRef.current;
    if (!container) return;

    let cleanupInteractions: (() => void) | undefined;
    let cancelled = false;

    document.body.classList.remove("show-expert");
    container.innerHTML = html;

    loadScript("https://cdn.plot.ly/plotly-2.35.2.min.js")
      .then(() => {
        if (cancelled) return;
        runInlineScripts(container);
        cleanupInteractions = attachReportInteractions(container);
      })
      .catch(() => {
        if (!cancelled) {
          cleanupInteractions = attachReportInteractions(container);
        }
      });

    return () => {
      cancelled = true;
      cleanupInteractions?.();
      container.innerHTML = "";
    };
  }, [html, symbol]);

  return (
    <>
      <link rel="stylesheet" href="/reports/static/report.css" />
      <div ref={containerRef} />
    </>
  );
}
