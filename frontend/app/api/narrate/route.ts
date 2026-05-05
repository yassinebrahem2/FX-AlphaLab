import { GoogleGenerativeAI } from "@google/generative-ai";
import { NextRequest, NextResponse } from "next/server";

const PROMPT = `You are a concise FX market analyst. Given the following raw coordinator signal context (JSON), write 2–3 short sentences in plain English explaining what's happening in the market right now, what the top trade idea is, and why. Be direct, no jargon. No bullet points.`;

export async function POST(req: NextRequest) {
  const apiKey = process.env.GEMINI_API_KEY;
  if (!apiKey) {
    return NextResponse.json({ error: "GEMINI_API_KEY not set" }, { status: 500 });
  }

  const { context } = await req.json();
  if (!context) {
    return NextResponse.json({ error: "missing context" }, { status: 400 });
  }

  const genAI = new GoogleGenerativeAI(apiKey);
  const model = genAI.getGenerativeModel({ model: "gemini-3.1-flash-lite-preview" });

  const result = await model.generateContent(`${PROMPT}\n\n${JSON.stringify(context)}`);
  const narrative = result.response.text();

  return NextResponse.json({ narrative });
}
