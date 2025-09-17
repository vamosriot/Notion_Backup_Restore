# Cloud Backup Setup Guide

## Option 1: Google Cloud Platform (Free Tier)
```bash
# 1. Create a Google Cloud VM (free tier: e2-micro)
gcloud compute instances create notion-backup \
    --zone=us-central1-a \
    --machine-type=e2-micro \
    --image-family=ubuntu-2004-lts \
    --image-project=ubuntu-os-cloud \
    --boot-disk-size=30GB

# 2. SSH into the instance
gcloud compute ssh notion-backup --zone=us-central1-a

# 3. Install Python and dependencies
sudo apt update
sudo apt install python3 python3-pip git -y
```

## Option 2: DigitalOcean Droplet ($6/month)
- Create a basic droplet (1GB RAM, 25GB SSD)
- More reliable than free tier
- Easy setup with pre-configured Python

## Option 3: GitHub Codespaces (Free 60 hours/month)
- Run directly in browser
- Pre-configured environment
- Can run in background

## Setup Steps for Any Cloud Option:

### 1. Clone your repository
```bash
git clone <your-repo-url>
cd Notion_Backup_Restore
```

### 2. Install dependencies
```bash
pip3 install -r requirements.txt  # if you have one
# or install manually:
pip3 install notion-client python-dotenv typer rich
```

### 3. Configure environment
```bash
# Copy your .env file or set variables
export NOTION_TOKEN="your_token_here"
export BACKUP_INCLUDE_BLOCKS=true
export BACKUP_OUTPUT_DIR="/tmp/notion_backups"
```

### 4. Run backup in background
```bash
# Use nohup to keep running after disconnect
nohup python3 backup.py main --verbose > backup.log 2>&1 &

# Check progress
tail -f backup.log
```

### 5. Upload to Google Drive
```bash
# Install rclone for Google Drive sync
curl https://rclone.org/install.sh | sudo bash

# Configure Google Drive
rclone config

# Sync backups to Google Drive
rclone sync /tmp/notion_backups gdrive:NotionBackups
```
