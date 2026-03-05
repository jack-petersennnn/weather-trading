#!/bin/bash
# Run position manager, check if notifications were generated, trigger agent if so
cd /home/ubuntu/.openclaw/workspace/weather-trading

python3 position_manager.py >> /tmp/pm_output.log 2>&1

# Check if notification file has content
if [ -f pm_notifications.json ] && [ -s pm_notifications.json ]; then
    NOTIFS=$(cat pm_notifications.json)
    if [ "$NOTIFS" != "[]" ]; then
        # Write to a file the agent can pick up on heartbeat
        echo "$NOTIFS" > /tmp/pm_alert.json
        # Clear notifications
        echo "[]" > pm_notifications.json
    fi
fi
