#!/bin/bash

# Run backup and compress immediately to save space
echo "🚀 Starting compressed Notion backup..."

# Run backup in background
caffeinate -d python3 backup.py main --verbose &
BACKUP_PID=$!

echo "✅ Backup started (PID: $BACKUP_PID)"
echo "🔄 Will compress backup when complete..."

# Wait for backup to complete
wait $BACKUP_PID

# Find the latest backup directory
LATEST_BACKUP=$(ls -td backups/backup_* 2>/dev/null | head -1)

if [ -n "$LATEST_BACKUP" ]; then
    echo "📦 Compressing backup: $LATEST_BACKUP"
    
    # Create compressed archive
    tar -czf "${LATEST_BACKUP}.tar.gz" -C backups "$(basename "$LATEST_BACKUP")"
    
    # Check compression ratio
    ORIGINAL_SIZE=$(du -sh "$LATEST_BACKUP" | cut -f1)
    COMPRESSED_SIZE=$(du -sh "${LATEST_BACKUP}.tar.gz" | cut -f1)
    
    echo "✅ Compression complete!"
    echo "📊 Original size: $ORIGINAL_SIZE"
    echo "📊 Compressed size: $COMPRESSED_SIZE"
    echo "📁 Compressed file: ${LATEST_BACKUP}.tar.gz"
    
    # Ask if user wants to delete original
    echo ""
    echo "💡 You can delete the original folder to save space:"
    echo "   rm -rf \"$LATEST_BACKUP\""
else
    echo "❌ No backup found to compress"
fi
