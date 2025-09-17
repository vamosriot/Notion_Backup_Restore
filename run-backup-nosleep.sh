#!/bin/bash

# Run backup while preventing Mac from sleeping
echo "🚀 Starting Notion backup (preventing sleep)..."
echo "💡 Your Mac will stay awake during backup"
echo "⏹️  Press Ctrl+C to cancel"
echo ""

# Prevent sleep while running backup
caffeinate -d python3 backup.py main --verbose

echo ""
echo "✅ Backup completed! Your Mac can sleep normally now."
