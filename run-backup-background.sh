#!/bin/bash

# Run backup in background - survives terminal closing
echo "🚀 Starting Notion backup in background..."

# Create logs directory
mkdir -p logs

# Run backup in background with nohup
nohup caffeinate -d python3 backup.py main --verbose > logs/backup-$(date +%Y%m%d-%H%M%S).log 2>&1 &

BACKUP_PID=$!
echo "✅ Backup started in background!"
echo "📋 Process ID: $BACKUP_PID"
echo "📁 Log file: logs/backup-$(date +%Y%m%d-%H%M%S).log"
echo ""
echo "🔍 To check progress:"
echo "   tail -f logs/backup-*.log"
echo ""
echo "⏹️  To stop backup:"
echo "   kill $BACKUP_PID"
echo ""
echo "💡 You can now close this terminal - backup will continue!"

# Save PID for easy stopping
echo $BACKUP_PID > logs/backup.pid
echo "📝 PID saved to logs/backup.pid"
