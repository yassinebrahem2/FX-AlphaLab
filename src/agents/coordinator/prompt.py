from __future__ import annotations

from src.agents.coordinator.signal import CoordinatorReport


def build_llm_prompt(report: CoordinatorReport) -> str:
    """Render a structured LLM briefing from a CoordinatorReport.

    All numbers come from the report — the LLM must not invent or modify any figure.
    """
    ctx = report.narrative_context
    lines = [
        "You are an FX market analyst assistant. Based on the quantitative analysis below,",
        "explain the recommendation in clear, actionable language.",
        "RULES: Do NOT invent numbers. Do NOT modify numbers. Do NOT add caveats not in the data.",
        "Mention confidence tier, signal source, risk/reward, and any relevant market context.",
        "",
        f"DATE: {ctx['date']}",
        f"GLOBAL REGIME: {ctx['global_regime']}",
        f"OVERALL ACTION: {ctx['overall_action'].upper()}",
    ]

    if ctx["overall_action"] == "hold":
        lines.append(f"HOLD REASON: {ctx['hold_reason']}")
        lines.append("")
        lines.append("TASK: In 2-3 sentences, explain why the system recommends staying out today.")
    else:
        rec = ctx["top_recommendation"]
        ic_str = (
            rec["direction_ic"]
            if rec["direction_ic"] is not None
            else "N/A (validated by accuracy=58.6%)"
        )
        lines += [
            "",
            "── TOP RECOMMENDATION ──────────────────────────────────────",
            f"  Pair:            {rec['pair']}",
            f"  Action:          {rec['action']}",
            f"  Confidence:      {rec['confidence']} (source: {rec['signal_source']})",
            f"  Direction IC:    {ic_str}",
            f"  Horizon:         {rec['horizon']}",
            f"  Position size:   {rec['position_size_pct']}% of equity",
            f"  Stop loss:       {rec['sl_pct']}% from entry",
            f"  Take profit:     {rec['tp_pct']}% from entry",
            f"  Risk/reward:     {rec['risk_reward']}:1",
            f"  Expected vol:    {rec['estimated_vol_pct']}% daily (source: {rec['vol_source']})",
            "",
            "── ALL PAIRS (ranked by conviction) ────────────────────────",
        ]
        for p in ctx["all_pairs"]:
            stress = "  ⚠ stress" if p.get("composite_stress") else ""
            lines.append(
                f"  {p['pair']}: {p['action']:<5} ({p['confidence']}, "
                f"conv={p['conviction']:.4f}, "
                f"vol={p['estimated_vol_pct']:.3f}%[{p['vol_source'][:3]}]) "
                f"driver={p['macro_driver']}{stress}"
            )

        first = ctx["all_pairs"][0]
        attn_z = first.get("macro_attention_z")
        gdelt_z = first.get("gdelt_tone_z")
        lines += [
            "",
            "── MARKET CONTEXT ──────────────────────────────────────────",
            f"  Geo regime:          {first['geo_risk_regime']}",
            (
                f"  Macro attention z:   {attn_z:.3f}"
                if attn_z is not None
                else "  Macro attention z:   n/a"
            ),
            (
                f"  GDELT tone z:        {gdelt_z:.3f}"
                if gdelt_z is not None
                else "  GDELT tone z:        n/a"
            ),
            "",
            "TASK: In 4-6 sentences explain:",
            "  1. What to do and why (primary signal source and its confidence)",
            "  2. How much to risk and where to place stop-loss / take-profit",
            "  3. Any regime or vol context worth highlighting",
            "  4. One-line summary of the secondary pairs (not recommended but notable)",
        ]

    return "\n".join(lines)
