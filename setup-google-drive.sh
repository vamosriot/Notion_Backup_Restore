#!/bin/bash

# Setup script for Google Drive backup

echo "🔧 Setting up Google Drive backup..."

# Check if Google Drive is installed
if [ -d "/Applications/Google Drive.app" ] || [ -d "/Applications/Google Drive File Stream.app" ]; then
    echo "✅ Google Drive app found"
    
    # Find Google Drive folder
    GDRIVE_PATH=""
    if [ -d "$HOME/Google Drive" ]; then
        GDRIVE_PATH="$HOME/Google Drive"
    elif [ -d "$HOME/GoogleDrive" ]; then
        GDRIVE_PATH="$HOME/GoogleDrive"
    elif [ -d "/Volumes/GoogleDrive" ]; then
        GDRIVE_PATH="/Volumes/GoogleDrive"
    fi
    
    if [ -n "$GDRIVE_PATH" ]; then
        echo "✅ Google Drive folder found: $GDRIVE_PATH"
        
        # Create backup directory in Google Drive
        mkdir -p "$GDRIVE_PATH/NotionBackups"
        echo "✅ Created backup directory: $GDRIVE_PATH/NotionBackups"
        
        # Update .env file
        echo ""
        echo "📝 To use Google Drive, update your .env file:"
        echo "BACKUP_OUTPUT_DIR=\"$GDRIVE_PATH/NotionBackups\""
        echo ""
        echo "Or run with command line:"
        echo "python3 backup.py main --output-dir \"$GDRIVE_PATH/NotionBackups\" --verbose"
        
    else
        echo "❌ Google Drive folder not found"
        echo "Please make sure Google Drive is syncing to your local machine"
    fi
else
    echo "❌ Google Drive app not found"
    echo ""
    echo "🔧 Alternative: Install rclone for Google Drive sync"
    echo "1. Install rclone: brew install rclone"
    echo "2. Configure: rclone config"
    echo "3. Sync after backup: rclone sync ./backups gdrive:NotionBackups"
fi

echo ""
echo "🚀 Cloud alternatives:"
echo "1. Google Cloud Platform (free tier)"
echo "2. DigitalOcean ($6/month)"
echo "3. GitHub Codespaces (60 hours free/month)"
echo ""
echo "See cloud-setup.md for detailed instructions"
