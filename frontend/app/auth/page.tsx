"use client";

import { useState } from "react";
import { useRouter } from "next/navigation";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import {
  Card,
  CardContent,
  CardDescription,
  CardFooter,
  CardHeader,
  CardTitle,
} from "@/components/ui/card";
import { Checkbox } from "@/components/ui/checkbox";
import { Input } from "@/components/ui/input";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { Shield, Lock, ChartLine, Sparkles } from "lucide-react";

const API_BASE = process.env.NEXT_PUBLIC_API_URL ?? "http://localhost:8000";

interface TokenResponse {
  access_token: string;
  refresh_token: string;
  token_type: string;
  expires_in_seconds: number;
  user: {
    id: number;
    email: string;
    full_name: string | null;
    role: string;
    is_active: boolean;
    created_at: string;
    last_login_at: string | null;
  };
}

function storeTokens(data: TokenResponse) {
  localStorage.setItem("access_token", data.access_token);
  localStorage.setItem("refresh_token", data.refresh_token);
  localStorage.setItem("user", JSON.stringify(data.user));
}

export default function AuthPage() {
  const router = useRouter();

  // ── Login state ────────────────────────────────────────────────────────────
  const [loginEmail, setLoginEmail] = useState("");
  const [loginPassword, setLoginPassword] = useState("");
  const [loginLoading, setLoginLoading] = useState(false);
  const [loginError, setLoginError] = useState<string | null>(null);

  // ── Signup state ───────────────────────────────────────────────────────────
  const [signupName, setSignupName] = useState("");
  const [signupEmail, setSignupEmail] = useState("");
  const [signupPassword, setSignupPassword] = useState("");
  const [signupRole, setSignupRole] = useState("");
  const [signupLoading, setSignupLoading] = useState(false);
  const [signupError, setSignupError] = useState<string | null>(null);
  const [signupAgreed, setSignupAgreed] = useState(false);

  async function handleLogin(e: React.FormEvent) {
    e.preventDefault();
    setLoginError(null);
    setLoginLoading(true);
    try {
      const res = await fetch(`${API_BASE}/auth/login`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ email: loginEmail, password: loginPassword }),
      });
      const data = await res.json();
      if (!res.ok) {
        setLoginError(data.detail ?? "Login failed");
        return;
      }
      storeTokens(data as TokenResponse);
      router.push("/");
    } catch {
      setLoginError("Cannot reach server. Is the backend running?");
    } finally {
      setLoginLoading(false);
    }
  }

  async function handleSignup(e: React.FormEvent) {
    e.preventDefault();
    setSignupError(null);
    if (!signupAgreed) {
      setSignupError("You must agree to the compliance policy.");
      return;
    }
    setSignupLoading(true);
    try {
      const res = await fetch(`${API_BASE}/auth/signup`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          email: signupEmail,
          password: signupPassword,
          full_name: signupName || null,
          role: signupRole || null,
        }),
      });
      const data = await res.json();
      if (!res.ok) {
        // Pydantic validation errors come as { detail: [...] }
        if (Array.isArray(data.detail)) {
          setSignupError(data.detail.map((d: { msg: string }) => d.msg).join("; "));
        } else {
          setSignupError(data.detail ?? "Signup failed");
        }
        return;
      }
      storeTokens(data as TokenResponse);
      router.push("/");
    } catch {
      setSignupError("Cannot reach server. Is the backend running?");
    } finally {
      setSignupLoading(false);
    }
  }

  return (
    <main className="relative h-screen overflow-hidden bg-background text-foreground">
      <div className="pointer-events-none absolute inset-0">
        <div className="absolute -top-24 left-1/2 h-80 w-[520px] -translate-x-1/2 rounded-full bg-primary/10 blur-3xl" />
        <div className="absolute right-24 top-24 h-52 w-52 rounded-full bg-[var(--buy)]/12 blur-3xl" />
        <div className="absolute bottom-0 right-0 h-64 w-64 rounded-full bg-[var(--buy)]/10 blur-3xl" />
        <div className="absolute inset-0 bg-[linear-gradient(90deg,rgba(31,74,168,0.05)_1px,transparent_1px),linear-gradient(180deg,rgba(31,74,168,0.05)_1px,transparent_1px)] bg-[size:36px_36px] opacity-40" />
        <div className="absolute inset-0 bg-[radial-gradient(circle_at_top,rgba(31,74,168,0.12),transparent_55%),radial-gradient(circle_at_bottom,rgba(13,148,136,0.1),transparent_45%)]" />
      </div>

      <div className="relative mx-auto flex h-full w-full max-w-6xl flex-col px-6 py-5">
        {/* Header */}
        <header className="flex shrink-0 items-center justify-between">
          <div className="flex items-center gap-3">
            <div className="flex h-9 w-9 items-center justify-center rounded-md bg-primary">
              <span className="text-sm font-bold text-primary-foreground">FX</span>
            </div>
            <div>
              <p className="text-sm font-semibold">AlphaLab</p>
              <p className="text-[10px] uppercase tracking-widest text-muted-foreground">Intelligent FX Platform</p>
            </div>
          </div>
          <div className="flex items-center gap-3">
            <div className="flex items-center gap-2 rounded-full border border-border bg-card/80 px-3 py-1 text-[10px] uppercase tracking-widest text-muted-foreground shadow-[var(--card-shadow)]">
              <span className="h-1.5 w-1.5 rounded-full bg-[var(--profit)]" />
              System online
            </div>
            <Badge className="h-6 bg-[var(--long)] text-[10px] text-white">LIVE</Badge>
          </div>
        </header>

        {/* Body */}
        <div className="mt-5 grid flex-1 gap-8 lg:grid-cols-[1.1fr_0.9fr] lg:items-center">
          {/* Left — value proposition */}
          <section className="space-y-4">
            <div className="space-y-2">
              <Badge variant="secondary" className="h-6 bg-secondary text-[10px] uppercase tracking-widest">
                Institutional-grade FX intelligence
              </Badge>
              <h1 className="text-3xl font-semibold leading-tight md:text-4xl">
                Precision insights, calibrated for every opportunity.
              </h1>
              <p className="text-sm text-muted-foreground">
                Access the multi-agent workspace, unified order desk, and real-time signal telemetry — all in one platform.
              </p>
            </div>

            {/* Market Pulse */}
            <div className="rounded-xl border border-border bg-card/80 p-4 shadow-[var(--card-shadow)]">
              <div className="flex items-center justify-between mb-3">
                <div>
                  <p className="text-[10px] uppercase tracking-widest text-muted-foreground">Market Pulse</p>
                  <p className="mt-0.5 text-sm font-semibold">EURUSD momentum building</p>
                </div>
                <Badge className="bg-primary/10 text-primary text-[10px]">Updated 2m</Badge>
              </div>
              <div className="grid gap-2 sm:grid-cols-3">
                <div className="rounded-lg border border-border bg-background/60 p-2.5">
                  <p className="text-[10px] uppercase tracking-widest text-muted-foreground">Momentum</p>
                  <p className="mt-0.5 text-base font-semibold text-primary">Strong</p>
                </div>
                <div className="rounded-lg border border-border bg-background/60 p-2.5">
                  <p className="text-[10px] uppercase tracking-widest text-muted-foreground">Regime</p>
                  <p className="mt-0.5 text-base font-semibold text-[var(--profit)]">Trending</p>
                </div>
                <div className="rounded-lg border border-border bg-background/60 p-2.5">
                  <p className="text-[10px] uppercase tracking-widest text-muted-foreground">Agents</p>
                  <p className="mt-0.5 text-base font-semibold">4 aligned</p>
                </div>
              </div>
            </div>

            {/* Feature cards */}
            <div className="grid gap-3 md:grid-cols-2">
              <div className="rounded-lg border border-border bg-card/80 p-3 shadow-[var(--card-shadow)]">
                <div className="flex items-center gap-2 text-xs font-semibold uppercase tracking-widest text-muted-foreground">
                  <Shield className="h-3.5 w-3.5 text-primary" />
                  Security
                </div>
                <p className="mt-1.5 text-xs">Enterprise-grade protection with seamless device trust and session continuity.</p>
                <p className="mt-2 text-[10px] uppercase tracking-widest text-[var(--profit)]">SOC 2 ready</p>
              </div>
              <div className="rounded-lg border border-border bg-card/80 p-3 shadow-[var(--card-shadow)]">
                <div className="flex items-center gap-2 text-xs font-semibold uppercase tracking-widest text-muted-foreground">
                  <ChartLine className="h-3.5 w-3.5 text-primary" />
                  Performance
                </div>
                <p className="mt-1.5 text-xs">99.98% uptime with sub-80ms signal routing and live data streaming.</p>
                <p className="mt-2 font-mono text-[10px] text-[var(--profit)]">Latency: 74ms</p>
              </div>
              <div className="rounded-lg border border-border bg-card/80 p-3 shadow-[var(--card-shadow)]">
                <div className="flex items-center gap-2 text-xs font-semibold uppercase tracking-widest text-muted-foreground">
                  <Lock className="h-3.5 w-3.5 text-primary" />
                  Privacy
                </div>
                <p className="mt-1.5 text-xs">End-to-end encryption with zero data sharing and full ownership of your signals.</p>
                <p className="mt-2 text-[10px] uppercase tracking-widest text-[var(--profit)]">Fully encrypted</p>
              </div>
              <div className="rounded-lg border border-border bg-card/80 p-3 shadow-[var(--card-shadow)]">
                <div className="flex items-center gap-2 text-xs font-semibold uppercase tracking-widest text-muted-foreground">
                  <Sparkles className="h-3.5 w-3.5 text-primary" />
                  Alpha Signals
                </div>
                <p className="mt-1.5 text-xs">Macro, geo, sentiment and technical agents converging into one clear decision.</p>
                <p className="mt-2 font-mono text-[10px] text-primary">4 agents · always on</p>
              </div>
            </div>
          </section>

          {/* Right — auth card */}
          <section>
            <Card className="relative overflow-hidden border-border bg-card/90 shadow-[var(--card-shadow)] backdrop-blur">
              <div className="absolute inset-x-0 top-0 h-1 bg-gradient-to-r from-primary via-[var(--buy)]/70 to-transparent" />
              <CardHeader className="space-y-1 pb-3">
                <CardTitle className="text-lg">Welcome to AlphaLab</CardTitle>
                <CardDescription>
                  Sign in or create your account to start trading smarter today.
                </CardDescription>
              </CardHeader>
              <CardContent className="pb-3">
                <Tabs defaultValue="login" className="w-full">
                  <TabsList className="w-full">
                    <TabsTrigger value="login" className="text-xs">Sign in</TabsTrigger>
                    <TabsTrigger value="signup" className="text-xs">Create account</TabsTrigger>
                  </TabsList>

                  {/* ── Login tab ── */}
                  <TabsContent value="login" className="mt-3">
                    <form onSubmit={handleLogin} className="space-y-3">
                      <div className="space-y-1.5">
                        <label className="text-[10px] font-semibold uppercase tracking-widest text-muted-foreground" htmlFor="login-email">
                          Email
                        </label>
                        <Input
                          id="login-email"
                          type="email"
                          placeholder="trader@alphalab.io"
                          value={loginEmail}
                          onChange={(e) => setLoginEmail(e.target.value)}
                          required
                          autoComplete="email"
                        />
                      </div>
                      <div className="space-y-1.5">
                        <label className="text-[10px] font-semibold uppercase tracking-widest text-muted-foreground" htmlFor="login-password">
                          Password
                        </label>
                        <Input
                          id="login-password"
                          type="password"
                          placeholder="••••••••"
                          value={loginPassword}
                          onChange={(e) => setLoginPassword(e.target.value)}
                          required
                          autoComplete="current-password"
                        />
                      </div>
                      <div className="flex items-center justify-between text-xs">
                        <label className="flex items-center gap-2 text-muted-foreground">
                          <Checkbox />
                          Remember this device
                        </label>
                        <button type="button" className="text-primary hover:underline">Forgot password?</button>
                      </div>

                      {loginError && (
                        <p className="rounded-md bg-destructive/10 px-3 py-2 text-xs text-destructive">
                          {loginError}
                        </p>
                      )}

                      <Button type="submit" className="w-full" disabled={loginLoading}>
                        {loginLoading ? "Signing in…" : "Access Dashboard"}
                      </Button>
                    </form>
                  </TabsContent>

                  {/* ── Signup tab ── */}
                  <TabsContent value="signup" className="mt-3">
                    <form onSubmit={handleSignup} className="space-y-3">
                      <div className="space-y-1.5">
                        <label className="text-[10px] font-semibold uppercase tracking-widest text-muted-foreground" htmlFor="signup-name">
                          Full name
                        </label>
                        <Input
                          id="signup-name"
                          placeholder="Alex Morgan"
                          value={signupName}
                          onChange={(e) => setSignupName(e.target.value)}
                          autoComplete="name"
                        />
                      </div>
                      <div className="space-y-1.5">
                        <label className="text-[10px] font-semibold uppercase tracking-widest text-muted-foreground" htmlFor="signup-email">
                          Work email
                        </label>
                        <Input
                          id="signup-email"
                          type="email"
                          placeholder="alex@fund.io"
                          value={signupEmail}
                          onChange={(e) => setSignupEmail(e.target.value)}
                          required
                          autoComplete="email"
                        />
                      </div>
                      <div className="space-y-1.5">
                        <label className="text-[10px] font-semibold uppercase tracking-widest text-muted-foreground" htmlFor="signup-password">
                          Password
                        </label>
                        <Input
                          id="signup-password"
                          type="password"
                          placeholder="Minimum 12 characters"
                          value={signupPassword}
                          onChange={(e) => setSignupPassword(e.target.value)}
                          required
                          autoComplete="new-password"
                        />
                      </div>
                      <div className="space-y-1.5">
                        <label className="text-[10px] font-semibold uppercase tracking-widest text-muted-foreground" htmlFor="signup-role">
                          Role
                        </label>
                        <Input
                          id="signup-role"
                          placeholder="e.g. Portfolio Manager"
                          value={signupRole}
                          onChange={(e) => setSignupRole(e.target.value)}
                        />
                      </div>
                      <label className="flex items-start gap-2 text-xs text-muted-foreground">
                        <Checkbox
                          className="mt-0.5"
                          checked={signupAgreed}
                          onCheckedChange={(v) => setSignupAgreed(v === true)}
                        />
                        I agree to the FX-AlphaLab terms of service and platform usage policy.
                      </label>

                      {signupError && (
                        <p className="rounded-md bg-destructive/10 px-3 py-2 text-xs text-destructive">
                          {signupError}
                        </p>
                      )}

                      <Button type="submit" className="w-full" disabled={signupLoading}>
                        {signupLoading ? "Creating account…" : "Get Started"}
                      </Button>
                    </form>
                  </TabsContent>
                </Tabs>
              </CardContent>
              <CardFooter className="border-t border-border pt-3">
                <div className="flex w-full items-center justify-between text-[11px] text-muted-foreground">
                  <span>Need a guided walkthrough?</span>
                  <button className="text-primary hover:underline">Contact our team</button>
                </div>
              </CardFooter>
            </Card>
          </section>
        </div>
      </div>
    </main>
  );
}
