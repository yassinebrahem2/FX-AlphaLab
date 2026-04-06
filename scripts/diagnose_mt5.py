"""Quick diagnostic to test MT5 connection."""
import time

try:
    import MetaTrader5 as mt5
    print("✓ MetaTrader5 module imported successfully")
except ImportError as e:
    print(f"✗ Failed to import MetaTrader5: {e}")
    exit(1)

print("\nAttempting MT5 initialization...")

# Try direct initialize
for attempt in range(1, 4):
    print(f"  Attempt {attempt}/3...", end=" ")
    if mt5.initialize():
        print("✓ SUCCESS")
        print(f"\nMT5 is connected:")
        print(f"  Terminal: {mt5.terminal_info()}")
        print(f"  Account: {mt5.account_info()}")
        
        # Try to get a symbol
        try:
            mt5.symbol_select("EURUSD", True)
            print(f"  EURUSD symbol: available")
        except Exception as e:
            print(f"  EURUSD symbol: {e}")
        
        mt5.shutdown()
        print("\n✓ MT5 shutdown successful")
        exit(0)
    else:
        error = mt5.last_error()
        print(f"✗ Failed: {error}")
    
    if attempt < 3:
        print("  Waiting 2 seconds before retry...")
        time.sleep(2)

print("\n✗ All initialization attempts failed")
print("\nTroubleshooting:")
print("  1. Open MetaTrader 5 terminal manually")
print("  2. Log in with your account")
print("  3. Wait for terminal to fully load (check status bar)")
print("  4. Then rerun this diagnostic")
exit(1)
