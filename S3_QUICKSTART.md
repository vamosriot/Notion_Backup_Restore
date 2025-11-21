# S3 Backup & Restore - Quick Start

## 🚀 Quick Setup (5 minutes)

### 1. Install boto3

```bash
pip install boto3
```

### 2. Configure Environment

Add to your `.env` file:

```bash
# Required
S3_BUCKET_NAME=your-notion-backups
AWS_REGION=us-east-1
AWS_USE_IAM_ROLE=true

# Optional
S3_PREFIX=notion-backups/
```

### 3. Run Your First Backup

```bash
python backup_to_s3.py --verbose
```

That's it! Your backup is now in S3.

## 📦 Common Commands

### Backup

```bash
# Basic backup to S3
python backup_to_s3.py

# Keep local copy
python backup_to_s3.py --keep-local

# With page content
python backup_to_s3.py --include-blocks --verbose
```

### Restore

```bash
# Interactive restore (shows menu)
python restore_from_s3.py

# Restore specific backup
python restore_from_s3.py backup_20231215_143022

# Dry run first
python restore_from_s3.py --dry-run
```

### List Backups

```bash
# List all backups
python backup_to_s3.py list
python restore_from_s3.py list

# Get backup details
python restore_from_s3.py info backup_20231215_143022
```

## 🔐 Authentication Methods

### IAM Role (Recommended)

**For EKS, ECS, EC2:**

```bash
# .env
S3_BUCKET_NAME=your-bucket
AWS_REGION=us-east-1
AWS_USE_IAM_ROLE=true
```

No credentials needed! Uses OIDC/IRSA.

### Access Keys

**For local development:**

```bash
# .env
S3_BUCKET_NAME=your-bucket
AWS_REGION=us-east-1
AWS_ACCESS_KEY_ID=AKIAIOSFODNN7EXAMPLE
AWS_SECRET_ACCESS_KEY=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY
AWS_USE_IAM_ROLE=false
```

## 📊 What Gets Uploaded

```
S3 Bucket
└── notion-backups/
    ├── index.json                    # Searchable backup index
    └── 2024/01/15/
        └── backup_20240115_143022.zip  # Compressed backup
            ├── databases/
            │   ├── documentation_schema.json
            │   ├── documentation_data.json
            │   ├── tasks_schema.json
            │   ├── tasks_data.json
            │   └── ...
            ├── manifest.json
            └── validation_report.json
```

## 🔄 Automation Examples

### Daily Cron Job

```bash
# Add to crontab (crontab -e)
0 2 * * * cd /path/to/Notion_Backup_Restore && python backup_to_s3.py
```

### Kubernetes CronJob

```yaml
apiVersion: batch/v1
kind: CronJob
metadata:
  name: notion-backup
spec:
  schedule: "0 2 * * *"
  jobTemplate:
    spec:
      template:
        spec:
          serviceAccountName: notion-backup-sa
          containers:
          - name: backup
            image: python:3.11
            command: 
            - /bin/bash
            - -c
            - |
              pip install boto3 notion-client python-dotenv typer rich
              python backup_to_s3.py
            env:
            - name: S3_BUCKET_NAME
              value: "your-bucket"
            - name: AWS_USE_IAM_ROLE
              value: "true"
            - name: NOTION_TOKEN
              valueFrom:
                secretKeyRef:
                  name: notion-secrets
                  key: token
          restartPolicy: OnFailure
```

## 💰 Cost Estimate

For a typical Notion workspace:
- **Backup size**: ~50-100 MB compressed
- **Daily backups**: ~3 GB/month
- **S3 Standard**: ~$0.07/month
- **With lifecycle policies**: ~$0.03/month

## ⚡ Quick Troubleshooting

### "Access Denied"

```bash
# Check IAM role
aws sts get-caller-identity

# Test S3 access
aws s3 ls s3://your-bucket/
```

### "Bucket Not Found"

```bash
# Create bucket
aws s3 mb s3://your-bucket --region us-east-1
```

### "Module not found: boto3"

```bash
pip install boto3
```

## 📚 Full Documentation

See [S3_BACKUP_GUIDE.md](S3_BACKUP_GUIDE.md) for:
- Detailed IAM setup
- Security best practices
- Cost optimization
- Advanced configurations
- Troubleshooting guide

## 🎯 Next Steps

1. ✅ Set up automated daily backups
2. ✅ Configure S3 lifecycle policies
3. ✅ Test restore process
4. ✅ Set up monitoring/alerts
5. ✅ Document your backup strategy

## 🆘 Need Help?

- Check [S3_BACKUP_GUIDE.md](S3_BACKUP_GUIDE.md) for detailed docs
- Run with `--debug` flag for detailed logs
- Check AWS CloudWatch logs
- Review IAM permissions

