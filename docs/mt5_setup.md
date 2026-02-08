\# MetaTrader 5 (MT5) Setup Guide



This document explains how to install and configure MetaTrader 5

for use with the FX-AlphaLab data pipelines.



---



\## 1. Install MetaTrader 5



\### Windows

1\. Download MT5 from the official site:

&nbsp;  https://www.metatrader5.com

2\. Run the installer

3\. Launch MetaTrader 5



⚠️ MT5 must be installed locally (not web version).



---



\## 2. Create a Demo Account



1\. Open MT5

2\. File → Open an Account

3\. Choose a broker (any demo broker is fine)

4\. Select \*\*Demo Account\*\*

5\. Complete registration

6\. Log in to the demo account



MT5 must show:

\- Account number

\- Balance

\- Market Watch symbols



---



\## 3. Enable Required Symbols



In MT5:

1\. Open \*\*Market Watch\*\* (Ctrl+M)

2\. Right-click → Symbols

3\. Enable:

&nbsp;  - EURUSD

&nbsp;  - GBPUSD

&nbsp;  - USDJPY

&nbsp;  - USDCHF



Ensure prices are updating.



---



\## 4. Python Environment Setup



Activate your virtual environment:



```bash

venv\\Scripts\\activate

Install dependency:



bash

Copier le code

pip install MetaTrader5

5\. Verify MT5 Python Connectivity

Run this test in Python:



python

Copier le code

import MetaTrader5 as mt5



if not mt5.initialize():

&nbsp;   raise RuntimeError("MT5 initialization failed")



print("MT5 version:", mt5.version())

mt5.shutdown()

Expected:



No errors



MT5 version printed



6\. Run the Price Pipeline

From project root:



bash

Copier le code

python src/pipelines/price/run\_price\_pipeline.py

Expected:



OHLC CSV files in data/raw/price



Clean CSV files in data/clean/price



No MT5 errors in console



7\. Common Issues \& Fixes

MT5 initialize() returns False

Ensure MT5 terminal is installed



Ensure MT5 is logged in



Restart MT5 and retry



Run Python as the same user who installed MT5



No data returned

Check symbol name (EURUSD, not EUR/USD)



Ensure Market Watch shows live prices



Check broker availability



Permission errors

Close MT5 and re-open



Restart terminal



Avoid running MT5 as admin if Python is not







