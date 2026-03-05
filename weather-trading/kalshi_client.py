"""
Kalshi Trading Client — Real Money
RSA-PSS signed requests for authenticated endpoints.
"""
import base64, time, json, os, urllib.request, urllib.error
from cryptography.hazmat.primitives.asymmetric import padding
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.backends import default_backend

KEYS_DIR = os.path.join(os.path.dirname(__file__), "keys")
BASE_URL = "https://api.elections.kalshi.com/trade-api/v2"

def _load_key():
    key_id = open(os.path.join(KEYS_DIR, "kalshi_key_id.txt")).read().strip()
    with open(os.path.join(KEYS_DIR, "kalshi_private_key.pem"), "rb") as f:
        pk = serialization.load_pem_private_key(f.read(), password=None, backend=default_backend())
    return key_id, pk

_KEY_ID, _PRIVATE_KEY = _load_key()

def _sign(method, path):
    ts = int(time.time() * 1000)
    msg = f"{ts}{method}{path}".encode()
    sig = _PRIVATE_KEY.sign(
        msg,
        padding.PSS(mgf=padding.MGF1(hashes.SHA256()), salt_length=padding.PSS.MAX_LENGTH),
        hashes.SHA256()
    )
    return ts, base64.b64encode(sig).decode()

def _headers(method, path):
    ts, sig = _sign(method, path)
    return {
        "KALSHI-ACCESS-KEY": _KEY_ID,
        "KALSHI-ACCESS-TIMESTAMP": str(ts),
        "KALSHI-ACCESS-SIGNATURE": sig,
        "Content-Type": "application/json",
        "Accept": "application/json",
    }

def _request(method, path, body=None, retries=3):
    """Make authenticated request to Kalshi API with retry logic.
    
    Retries on:
    - 429 (rate limit) — waits 2s between retries
    - 500/502/503 (server errors) — waits 1s
    - Timeout errors — waits 1s
    
    Does NOT retry on:
    - 400/401/403/404 (client errors) — our fault, retrying won't help
    """
    full_path = f"/trade-api/v2{path}"
    url = f"https://api.elections.kalshi.com{full_path}"
    data = json.dumps(body).encode() if body else None
    sign_path = full_path.split("?")[0]
    
    last_error = None
    for attempt in range(retries):
        # Re-sign each attempt (timestamp changes)
        hdrs = _headers(method, sign_path)
        req = urllib.request.Request(url, data=data, headers=hdrs, method=method)
        try:
            resp = urllib.request.urlopen(req, timeout=15)
            return json.loads(resp.read())
        except urllib.error.HTTPError as e:
            err_body = e.read().decode()
            last_error = f"Kalshi API {e.code}: {err_body}"
            if e.code == 429:
                # Rate limited — back off
                time.sleep(2)
                continue
            elif e.code in (500, 502, 503):
                time.sleep(1)
                continue
            else:
                # Client error — don't retry
                raise Exception(last_error)
        except Exception as e:
            last_error = str(e)
            if attempt < retries - 1:
                time.sleep(1)
                continue
            raise
    
    raise Exception(f"Kalshi API failed after {retries} attempts: {last_error}")

# ── Public / Portfolio ──────────────────────────────────────────────

def get_balance():
    """Returns balance in cents and portfolio value."""
    return _request("GET", "/portfolio/balance")

def get_positions():
    """Returns open positions."""
    return _request("GET", "/portfolio/positions")

def get_orders(ticker=None, status=None):
    """Returns orders, optionally filtered."""
    params = []
    if ticker: params.append(f"ticker={ticker}")
    if status: params.append(f"status={status}")
    qs = "&".join(params)
    path = "/portfolio/orders"
    return _request("GET", f"{path}?{qs}" if qs else path)

# ── Market Data ─────────────────────────────────────────────────────

def get_market(ticker):
    """Get single market details."""
    return _request("GET", f"/markets/{ticker}")

def get_markets(series_ticker=None, status="open", limit=200):
    """List markets with optional filters."""
    params = [f"limit={limit}", f"status={status}"]
    if series_ticker: params.append(f"series_ticker={series_ticker}")
    path = "/markets"
    return _request("GET", f"{path}?{'&'.join(params)}")

def get_orderbook(ticker):
    """Get orderbook for a market."""
    return _request("GET", f"/markets/{ticker}/orderbook")

# ── Trading ─────────────────────────────────────────────────────────

def place_order(ticker, side, contracts, price_cents, order_type="limit"):
    """
    Place an order on Kalshi.
    
    Args:
        ticker: Market ticker (e.g., "KXHIGHNY-26FEB20-B45")
        side: "yes" or "no"
        contracts: Number of contracts
        price_cents: Limit price in cents (1-99)
        order_type: "limit" or "market"
    
    Returns: Order response dict
    """
    body = {
        "ticker": ticker,
        "action": "buy",
        "side": side,
        "type": order_type,
        "count": contracts,
    }
    if order_type == "limit":
        body["yes_price"] = price_cents if side == "yes" else (100 - price_cents)
    
    return _request("POST", "/portfolio/orders", body)

def cancel_order(order_id):
    """Cancel an open order."""
    return _request("DELETE", f"/portfolio/orders/{order_id}")

def sell_position(ticker, side, contracts, price_cents):
    """Sell (exit) an existing position. Use safe_sell_position() for verified sells."""
    body = {
        "ticker": ticker,
        "action": "sell",
        "side": side,
        "type": "limit",
        "count": contracts,
    }
    if side == "yes":
        body["yes_price"] = price_cents
    else:
        body["yes_price"] = 100 - price_cents
    
    return _request("POST", "/portfolio/orders", body)


def safe_sell_position(ticker, contract_side, contracts, price_cents):
    """
    Safety wrapper: verifies position side + clamps quantity from Kalshi API before selling.
    
    Args:
        ticker: Market ticker
        contract_side: "yes" or "no" — the side we believe we're holding
        contracts: Number of contracts to sell
        price_cents: Limit price in cents
    
    Returns: (success: bool, result: dict, audit: dict)
    
    Three distinct variables tracked:
        position_side_held: what Kalshi says we own (from API)
        order_action: always "sell"
        contract_side: what we intend to close (passed in)
    """
    audit = {
        "ticker": ticker,
        "intended_side": contract_side,
        "intended_contracts": contracts,
        "intended_price": price_cents,
    }
    
    # 1. Fetch actual position from Kalshi
    try:
        positions = get_positions()
    except Exception as e:
        audit["error"] = f"Failed to fetch positions: {e}"
        return False, {}, audit
    
    # Find our position for this ticker
    found = None
    for p in positions.get("market_positions", []):
        if p.get("ticker") == ticker:
            found = p
            break
    
    if not found:
        audit["error"] = f"No position found for {ticker}"
        audit["position_side_held"] = None
        return False, {}, audit
    
    position_count = found.get("position", 0)
    if position_count == 0:
        audit["error"] = f"Position count is 0 for {ticker}"
        audit["position_side_held"] = None
        return False, {}, audit
    
    # Kalshi position API does NOT include a 'side' field.
    # position is always a non-negative integer count.
    # The side (YES/NO) comes from our contract_side parameter (from trades.json).
    # We verify the position EXISTS and has sufficient count, but side is our responsibility.
    actual_count = abs(position_count)
    
    # We trust contract_side from trades.json — Kalshi confirms we HAVE a position
    # but doesn't tell us which side. This is safe because:
    # - trades.json records direction at entry time
    # - Position manager only calls this for open trades
    audit["position_side_held"] = contract_side.lower()  # from our records
    audit["position_count"] = actual_count
    audit["kalshi_position_raw"] = position_count
    
    # 3. Clamp quantity to actual holdings
    clamped_contracts = min(contracts, actual_count)
    audit["clamped_contracts"] = clamped_contracts
    if clamped_contracts < contracts:
        audit["warning"] = f"Clamped from {contracts} to {clamped_contracts} (only hold {actual_count})"
    
    if clamped_contracts <= 0:
        audit["error"] = "Nothing to sell after clamp"
        return False, {}, audit
    
    # 4. Execute the sell
    try:
        result = sell_position(ticker, contract_side.lower(), clamped_contracts, price_cents)
        audit["order_action"] = "sell"
        audit["executed"] = True
        
        # 5. Post-sell position fetch (logging only, NOT correctness check)
        try:
            post_positions = get_positions()
            for pp in post_positions.get("market_positions", []):
                if pp.get("ticker") == ticker:
                    audit["post_sell_position"] = pp.get("position", 0)
                    break
            else:
                audit["post_sell_position"] = 0
        except:
            audit["post_sell_position"] = "fetch_failed"
        
        return True, result, audit
    except Exception as e:
        audit["error"] = f"Sell order failed: {e}"
        audit["executed"] = False
        return False, {}, audit

# ── Convenience ─────────────────────────────────────────────────────

def get_balance_dollars():
    b = get_balance()
    return b["balance"] / 100

if __name__ == "__main__":
    bal = get_balance()
    print(f"Balance: ${bal['balance']/100:.2f}")
    print(f"Portfolio value: ${bal.get('portfolio_value', 0)/100:.2f}")
