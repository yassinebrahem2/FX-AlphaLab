"""
Root pytest configuration.

Stubs out optional heavy packages (selenium, undetected_chromedriver,
MetaTrader5) so that collectors which depend on them can still be imported
during testing without those packages being installed in the dev environment.
"""

import sys
import types


def _make_stub(name: str) -> types.ModuleType:
    """Return an empty module stub registered under *name*."""
    mod = types.ModuleType(name)
    sys.modules[name] = mod
    return mod


# --- selenium stubs -------------------------------------------------------
if "selenium" not in sys.modules:
    selenium = _make_stub("selenium")
    selenium_wd = _make_stub("selenium.webdriver")
    selenium_wd_chrome = _make_stub("selenium.webdriver.chrome")
    selenium_wd_chrome_opts = _make_stub("selenium.webdriver.chrome.options")
    selenium_wd_chrome_svc = _make_stub("selenium.webdriver.chrome.service")
    selenium_wd_common = _make_stub("selenium.webdriver.common")
    selenium_wd_common_by = _make_stub("selenium.webdriver.common.by")
    selenium_wd_support = _make_stub("selenium.webdriver.support")
    selenium_wd_support_ec = _make_stub("selenium.webdriver.support.expected_conditions")
    selenium_wd_support_ui = _make_stub("selenium.webdriver.support.ui")
    selenium_common = _make_stub("selenium.common")
    selenium_common_exc = _make_stub("selenium.common.exceptions")

    # Minimal attributes expected by collector code
    selenium_wd_chrome_opts.Options = type("Options", (), {})
    selenium_wd_chrome_svc.Service = type("Service", (), {})
    selenium_wd_common_by.By = type("By", (), {"CSS_SELECTOR": "", "XPATH": "", "ID": ""})
    selenium_wd_support_ui.WebDriverWait = type("WebDriverWait", (), {})
    selenium_common_exc.TimeoutException = Exception
    selenium_common_exc.WebDriverException = Exception
    selenium_common_exc.StaleElementReferenceException = Exception
    selenium_wd.webdriver = selenium_wd
    selenium.webdriver = selenium_wd

# --- undetected_chromedriver stub ----------------------------------------
if "undetected_chromedriver" not in sys.modules:
    uc = _make_stub("undetected_chromedriver")
    uc.Chrome = type("Chrome", (), {})
    uc.ChromeOptions = type("ChromeOptions", (), {})

# --- MetaTrader5 stub -----------------------------------------------------
if "MetaTrader5" not in sys.modules:
    mt5_stub = _make_stub("MetaTrader5")
    mt5_stub.initialize = lambda *a, **kw: False
    mt5_stub.shutdown = lambda: None
