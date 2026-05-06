"""Email service — sends transactional emails via SMTP (stdlib only, no extra deps)."""

from __future__ import annotations

import logging
import smtplib
import threading
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

from src.shared.config import Config

logger = logging.getLogger(__name__)


def _build_welcome_html(full_name: str, email: str) -> str:
    display_name = full_name.strip() if full_name else email.split("@")[0]
    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8" />
<meta name="viewport" content="width=device-width, initial-scale=1.0" />
<title>Welcome to FX-AlphaLab</title>
</head>
<body style="margin:0;padding:0;background:#0f1117;font-family:'Segoe UI',Arial,sans-serif;color:#e5e7eb;">
  <table width="100%" cellpadding="0" cellspacing="0" style="background:#0f1117;padding:40px 0;">
    <tr>
      <td align="center">
        <table width="580" cellpadding="0" cellspacing="0" style="background:#1a1d27;border-radius:12px;overflow:hidden;border:1px solid #2a2d3a;">

          <!-- Top accent bar -->
          <tr>
            <td style="background:linear-gradient(90deg,#1f4aa8,#0d9488 60%,transparent);height:4px;"></td>
          </tr>

          <!-- Header -->
          <tr>
            <td align="center" style="padding:36px 40px 24px;">
              <table cellpadding="0" cellspacing="0">
                <tr>
                  <td style="background:#1f4aa8;border-radius:8px;width:44px;height:44px;text-align:center;vertical-align:middle;">
                    <span style="color:#ffffff;font-weight:700;font-size:16px;line-height:44px;">FX</span>
                  </td>
                  <td style="padding-left:12px;vertical-align:middle;">
                    <p style="margin:0;font-size:18px;font-weight:700;color:#ffffff;">AlphaLab</p>
                    <p style="margin:0;font-size:10px;letter-spacing:2px;text-transform:uppercase;color:#6b7280;">Intelligent FX Platform</p>
                  </td>
                </tr>
              </table>
            </td>
          </tr>

          <!-- Hero -->
          <tr>
            <td style="padding:0 40px 32px;">
              <p style="margin:0 0 8px;font-size:26px;font-weight:700;color:#ffffff;line-height:1.3;">
                Welcome aboard, {display_name}!
              </p>
              <p style="margin:0;font-size:15px;color:#9ca3af;line-height:1.6;">
                Your FX-AlphaLab account is active and ready. You now have access to one of the most advanced
                AI-driven FX intelligence platforms available.
              </p>
            </td>
          </tr>

          <!-- Stats row -->
          <tr>
            <td style="padding:0 40px 32px;">
              <table width="100%" cellpadding="0" cellspacing="0">
                <tr>
                  <td width="32%" style="background:#0f1117;border-radius:8px;border:1px solid #2a2d3a;padding:16px;text-align:center;">
                    <p style="margin:0;font-size:10px;letter-spacing:1.5px;text-transform:uppercase;color:#6b7280;">Agents</p>
                    <p style="margin:6px 0 0;font-size:22px;font-weight:700;color:#1f4aa8;">4</p>
                    <p style="margin:2px 0 0;font-size:10px;color:#6b7280;">Always aligned</p>
                  </td>
                  <td width="4%"></td>
                  <td width="32%" style="background:#0f1117;border-radius:8px;border:1px solid #2a2d3a;padding:16px;text-align:center;">
                    <p style="margin:0;font-size:10px;letter-spacing:1.5px;text-transform:uppercase;color:#6b7280;">Uptime</p>
                    <p style="margin:6px 0 0;font-size:22px;font-weight:700;color:#0d9488;">99.98%</p>
                    <p style="margin:2px 0 0;font-size:10px;color:#6b7280;">Live system</p>
                  </td>
                  <td width="4%"></td>
                  <td width="32%" style="background:#0f1117;border-radius:8px;border:1px solid #2a2d3a;padding:16px;text-align:center;">
                    <p style="margin:0;font-size:10px;letter-spacing:1.5px;text-transform:uppercase;color:#6b7280;">Latency</p>
                    <p style="margin:6px 0 0;font-size:22px;font-weight:700;color:#0d9488;">74ms</p>
                    <p style="margin:2px 0 0;font-size:10px;color:#6b7280;">Signal routing</p>
                  </td>
                </tr>
              </table>
            </td>
          </tr>

          <!-- What you get -->
          <tr>
            <td style="padding:0 40px 32px;">
              <p style="margin:0 0 16px;font-size:13px;font-weight:600;letter-spacing:1px;text-transform:uppercase;color:#6b7280;">
                What's waiting for you
              </p>
              <table width="100%" cellpadding="0" cellspacing="0">
                <tr>
                  <td style="padding:10px 0;border-bottom:1px solid #2a2d3a;">
                    <table cellpadding="0" cellspacing="0">
                      <tr>
                        <td style="width:28px;font-size:16px;">📈</td>
                        <td>
                          <p style="margin:0;font-size:14px;font-weight:600;color:#ffffff;">Multi-agent Alpha Signals</p>
                          <p style="margin:2px 0 0;font-size:12px;color:#9ca3af;">Technical, macro, geopolitical and sentiment agents working in concert.</p>
                        </td>
                      </tr>
                    </table>
                  </td>
                </tr>
                <tr>
                  <td style="padding:10px 0;border-bottom:1px solid #2a2d3a;">
                    <table cellpadding="0" cellspacing="0">
                      <tr>
                        <td style="width:28px;font-size:16px;">⚡</td>
                        <td>
                          <p style="margin:0;font-size:14px;font-weight:600;color:#ffffff;">Real-time Market Intelligence</p>
                          <p style="margin:2px 0 0;font-size:12px;color:#9ca3af;">Live OHLCV data, macro calendars and institutional flow — all in one view.</p>
                        </td>
                      </tr>
                    </table>
                  </td>
                </tr>
                <tr>
                  <td style="padding:10px 0;">
                    <table cellpadding="0" cellspacing="0">
                      <tr>
                        <td style="width:28px;font-size:16px;">🔒</td>
                        <td>
                          <p style="margin:0;font-size:14px;font-weight:600;color:#ffffff;">Enterprise Security</p>
                          <p style="margin:2px 0 0;font-size:12px;color:#9ca3af;">End-to-end encrypted sessions, JWT token rotation, and full audit trail.</p>
                        </td>
                      </tr>
                    </table>
                  </td>
                </tr>
              </table>
            </td>
          </tr>

          <!-- CTA -->
          <tr>
            <td align="center" style="padding:0 40px 36px;">
              <a href="http://localhost:3000"
                 style="display:inline-block;background:#1f4aa8;color:#ffffff;font-size:14px;font-weight:600;
                        text-decoration:none;padding:14px 40px;border-radius:8px;letter-spacing:0.5px;">
                Open the Dashboard →
              </a>
            </td>
          </tr>

          <!-- Footer -->
          <tr>
            <td style="padding:20px 40px;border-top:1px solid #2a2d3a;">
              <p style="margin:0;font-size:11px;color:#4b5563;text-align:center;">
                You received this email because an account was created for <strong style="color:#6b7280;">{email}</strong>.<br/>
                FX-AlphaLab · Intelligent FX Platform
              </p>
            </td>
          </tr>

        </table>
      </td>
    </tr>
  </table>
</body>
</html>"""


def _send(to_email: str, full_name: str) -> None:
    """Send welcome email. Called from a background thread — never raises."""
    if not Config.SMTP_USER or not Config.SMTP_PASSWORD:
        logger.warning("SMTP not configured — skipping welcome email for %s", to_email)
        return

    try:
        msg = MIMEMultipart("alternative")
        msg["Subject"] = "Welcome to FX-AlphaLab — your account is live"
        msg["From"] = f"{Config.SMTP_FROM_NAME} <{Config.SMTP_USER}>"
        msg["To"] = to_email

        plain = (
            f"Welcome to FX-AlphaLab, {full_name or to_email}!\n\n"
            "Your account is active. Sign in at http://localhost:3000\n\n"
            "— The FX-AlphaLab Team"
        )
        msg.attach(MIMEText(plain, "plain"))
        msg.attach(MIMEText(_build_welcome_html(full_name or "", to_email), "html"))

        with smtplib.SMTP(Config.SMTP_HOST, Config.SMTP_PORT, timeout=10) as server:
            server.ehlo()
            server.starttls()
            server.login(Config.SMTP_USER, Config.SMTP_PASSWORD)
            server.sendmail(Config.SMTP_USER, to_email, msg.as_string())

        logger.info("Welcome email sent to %s", to_email)

    except Exception:
        logger.exception("Failed to send welcome email to %s", to_email)


def send_welcome_email(to_email: str, full_name: str | None = None) -> None:
    """Fire-and-forget welcome email in a background thread."""
    threading.Thread(
        target=_send,
        args=(to_email, full_name or ""),
        daemon=True,
        name=f"welcome-email-{to_email}",
    ).start()
