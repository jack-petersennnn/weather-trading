#!/bin/bash
# Full weather trading pipeline — runs scanner, analyzer, trader, dashboard
# Pure Python, $0 token cost
cd /home/ubuntu/.openclaw/workspace/weather-trading

echo "$(date -u '+%Y-%m-%d %H:%M:%S UTC') — Pipeline starting"

python3 run.py 2>&1

# If any trades were placed or notable events, write notification
python3 -c "
import json, os
log = 'live_trade_log.json'
notify = 'pm_notifications.json'
if os.path.exists(log):
    with open(log) as f:
        events = json.load(f)
    # Check for events in last 2 hours
    from datetime import datetime, timezone, timedelta
    cutoff = (datetime.now(timezone.utc) - timedelta(hours=2)).isoformat()
    recent = [e for e in events if e.get('logged_at','') > cutoff and e.get('type') == 'order_placed']
    if recent:
        notifs = []
        if os.path.exists(notify):
            try:
                with open(notify) as f: notifs = json.load(f)
            except: pass
        notifs.append({
            'ts': datetime.now(timezone.utc).isoformat(),
            'msg': f'📈 Pipeline placed {len(recent)} new trade(s)'
        })
        with open(notify, 'w') as f: json.dump(notifs, f)
" 2>/dev/null

echo "$(date -u '+%Y-%m-%d %H:%M:%S UTC') — Pipeline done"
