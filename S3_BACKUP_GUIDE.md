# S3 Backup & Restore Guide

This guide explains how to use the S3 backup and restore functionality to automatically upload your Notion backups to AWS S3 and restore them later.

## Features

- ✅ **Automatic Compression**: Backups are compressed to ZIP format before upload
- ✅ **IAM Role Support**: OIDC/IRSA authentication for secure, keyless access
- ✅ **Progress Tracking**: Real-time upload/download progress
- ✅ **Backup Index**: Automatic indexing of all backups in S3
- ✅ **Interactive Restore**: Select from available backups
- ✅ **Cleanup Options**: Automatically remove local files after upload

## Prerequisites

### 1. AWS S3 Bucket

Create an S3 bucket for your backups:

```bash
aws s3 mb s3://your-notion-backups --region us-east-1
```

### 2. IAM Permissions

Your IAM role or user needs these permissions:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "s3:PutObject",
        "s3:GetObject",
        "s3:ListBucket",
        "s3:DeleteObject"
      ],
      "Resource": [
        "arn:aws:s3:::your-notion-backups",
        "arn:aws:s3:::your-notion-backups/*"
      ]
    }
  ]
}
```

## Configuration

### Option 1: IAM Role (OIDC/IRSA) - Recommended

Perfect for running in:
- **Amazon EKS** (Kubernetes with IRSA)
- **Amazon ECS** (Container services)
- **EC2 instances** (with instance profiles)
- **AWS Lambda**

**Configuration (.env file):**

```bash
# S3 Configuration
S3_BUCKET_NAME=your-notion-backups
AWS_REGION=us-east-1
AWS_USE_IAM_ROLE=true
S3_PREFIX=notion-backups/

# Notion Configuration
NOTION_TOKEN=ntn_your_token_here
```

**No access keys needed!** The IAM role provides automatic authentication.

#### Setting up IRSA for EKS

1. **Create IAM OIDC provider for your cluster:**
```bash
eksctl utils associate-iam-oidc-provider --cluster=your-cluster --approve
```

2. **Create IAM role with trust policy:**
```bash
eksctl create iamserviceaccount \
  --name notion-backup-sa \
  --namespace default \
  --cluster your-cluster \
  --attach-policy-arn arn:aws:iam::aws:policy/AmazonS3FullAccess \
  --approve
```

3. **Use the service account in your pod:**
```yaml
apiVersion: v1
kind: Pod
metadata:
  name: notion-backup
spec:
  serviceAccountName: notion-backup-sa
  containers:
  - name: backup
    image: your-backup-image
    env:
    - name: AWS_USE_IAM_ROLE
      value: "true"
```

### Option 2: Access Keys

For local development or non-AWS environments:

**Configuration (.env file):**

```bash
# S3 Configuration
S3_BUCKET_NAME=your-notion-backups
AWS_ACCESS_KEY_ID=AKIAIOSFODNN7EXAMPLE
AWS_SECRET_ACCESS_KEY=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY
AWS_REGION=us-east-1
AWS_USE_IAM_ROLE=false
S3_PREFIX=notion-backups/

# Notion Configuration
NOTION_TOKEN=ntn_your_token_here
```

## Usage

### Backup to S3

#### Basic Backup

```bash
python backup_to_s3.py
```

This will:
1. Create a Notion backup
2. Compress it to a ZIP file
3. Upload to S3
4. Clean up local files (by default)

#### Backup with Options

```bash
# Keep local backup files after upload
python backup_to_s3.py --keep-local

# Include page content blocks
python backup_to_s3.py --include-blocks

# Verbose output
python backup_to_s3.py --verbose

# Debug mode
python backup_to_s3.py --debug

# Dry run (create backup but don't upload)
python backup_to_s3.py --dry-run
```

#### List S3 Backups

```bash
python backup_to_s3.py list
```

### Restore from S3

#### Interactive Restore

```bash
python restore_from_s3.py
```

This will:
1. Show you a list of available backups
2. Let you select which one to restore
3. Download and extract the backup
4. Restore to Notion

#### Restore Specific Backup

```bash
python restore_from_s3.py backup_20231215_143022
```

#### Restore with Options

```bash
# Dry run (download but don't restore)
python restore_from_s3.py --dry-run

# Specify parent page for restored databases
python restore_from_s3.py --parent-id abc123def456

# Keep downloaded files after restore
python restore_from_s3.py --keep-download

# Verbose output
python restore_from_s3.py --verbose

# Skip validation
python restore_from_s3.py --no-validate
```

#### List Available Backups

```bash
python restore_from_s3.py list
```

#### Get Backup Info

```bash
python restore_from_s3.py info backup_20231215_143022
```

## S3 Bucket Structure

Your S3 bucket will be organized like this:

```
s3://your-notion-backups/
└── notion-backups/
    ├── index.json                           # Index of all backups
    ├── 2024/
    │   ├── 01/
    │   │   ├── 15/
    │   │   │   ├── backup_20240115_143022.zip
    │   │   │   └── backup_20240115_183045.zip
    │   │   └── 16/
    │   │       └── backup_20240116_093012.zip
    │   └── 02/
    │       └── 01/
    │           └── backup_20240201_120000.zip
```

## Automation

### Scheduled Backups with Cron

```bash
# Add to crontab (crontab -e)
# Daily backup at 2 AM
0 2 * * * cd /path/to/Notion_Backup_Restore && /usr/bin/python3 backup_to_s3.py >> /var/log/notion-backup.log 2>&1
```

### Kubernetes CronJob

```yaml
apiVersion: batch/v1
kind: CronJob
metadata:
  name: notion-backup
spec:
  schedule: "0 2 * * *"  # Daily at 2 AM
  jobTemplate:
    spec:
      template:
        spec:
          serviceAccountName: notion-backup-sa
          containers:
          - name: backup
            image: your-backup-image
            command: ["python", "backup_to_s3.py"]
            env:
            - name: NOTION_TOKEN
              valueFrom:
                secretKeyRef:
                  name: notion-secrets
                  key: token
            - name: S3_BUCKET_NAME
              value: "your-notion-backups"
            - name: AWS_REGION
              value: "us-east-1"
            - name: AWS_USE_IAM_ROLE
              value: "true"
          restartPolicy: OnFailure
```

### GitHub Actions

```yaml
name: Notion Backup to S3

on:
  schedule:
    - cron: '0 2 * * *'  # Daily at 2 AM
  workflow_dispatch:

jobs:
  backup:
    runs-on: ubuntu-latest
    permissions:
      id-token: write  # For OIDC
      contents: read
    
    steps:
      - uses: actions/checkout@v4
      
      - name: Configure AWS credentials
        uses: aws-actions/configure-aws-credentials@v4
        with:
          role-to-assume: arn:aws:iam::123456789012:role/GitHubActionsRole
          aws-region: us-east-1
      
      - name: Set up Python
        uses: actions/setup-python@v4
        with:
          python-version: '3.11'
      
      - name: Install dependencies
        run: |
          pip install -e .
      
      - name: Run backup
        env:
          NOTION_TOKEN: ${{ secrets.NOTION_TOKEN }}
          S3_BUCKET_NAME: your-notion-backups
          AWS_USE_IAM_ROLE: true
        run: |
          python backup_to_s3.py --verbose
```

## Cost Optimization

### S3 Lifecycle Policies

Automatically transition old backups to cheaper storage:

```bash
aws s3api put-bucket-lifecycle-configuration \
  --bucket your-notion-backups \
  --lifecycle-configuration file://lifecycle.json
```

**lifecycle.json:**

```json
{
  "Rules": [
    {
      "Id": "Archive old backups",
      "Status": "Enabled",
      "Filter": {
        "Prefix": "notion-backups/"
      },
      "Transitions": [
        {
          "Days": 30,
          "StorageClass": "STANDARD_IA"
        },
        {
          "Days": 90,
          "StorageClass": "GLACIER"
        }
      ],
      "Expiration": {
        "Days": 365
      }
    }
  ]
}
```

This will:
- Keep backups in Standard storage for 30 days
- Move to Infrequent Access after 30 days (cheaper)
- Move to Glacier after 90 days (even cheaper)
- Delete backups older than 365 days

### Estimated Costs

Assuming 100 MB compressed backup per day:

| Storage Type | Monthly Cost (100 MB/day) |
|--------------|---------------------------|
| S3 Standard (30 days) | ~$0.07 |
| S3 IA (60 days) | ~$0.08 |
| S3 Glacier (275 days) | ~$0.11 |
| **Total** | **~$0.26/month** |

## Troubleshooting

### "Access Denied" Error

**For IAM Role:**
```bash
# Check if IAM role is properly attached
aws sts get-caller-identity

# Verify S3 permissions
aws s3 ls s3://your-notion-backups/
```

**For Access Keys:**
```bash
# Verify credentials
aws configure list

# Test S3 access
aws s3 ls s3://your-notion-backups/
```

### "Bucket Not Found" Error

```bash
# Create the bucket
aws s3 mb s3://your-notion-backups --region us-east-1

# Verify bucket exists
aws s3 ls
```

### Slow Upload/Download

- Check your internet connection
- Consider using S3 Transfer Acceleration:
  ```bash
  aws s3api put-bucket-accelerate-configuration \
    --bucket your-notion-backups \
    --accelerate-configuration Status=Enabled
  ```

### IAM Role Not Working

**For EKS:**
```bash
# Verify service account annotation
kubectl describe sa notion-backup-sa

# Check pod environment
kubectl exec -it your-pod -- env | grep AWS
```

**For EC2:**
```bash
# Verify instance profile
aws ec2 describe-instances --instance-ids i-1234567890abcdef0 \
  --query 'Reservations[0].Instances[0].IamInstanceProfile'
```

## Security Best Practices

1. **Use IAM Roles** instead of access keys whenever possible
2. **Enable bucket versioning** for backup protection:
   ```bash
   aws s3api put-bucket-versioning \
     --bucket your-notion-backups \
     --versioning-configuration Status=Enabled
   ```

3. **Enable server-side encryption**:
   ```bash
   aws s3api put-bucket-encryption \
     --bucket your-notion-backups \
     --server-side-encryption-configuration '{
       "Rules": [{
         "ApplyServerSideEncryptionByDefault": {
           "SSEAlgorithm": "AES256"
         }
       }]
     }'
   ```

4. **Block public access**:
   ```bash
   aws s3api put-public-access-block \
     --bucket your-notion-backups \
     --public-access-block-configuration \
       BlockPublicAcls=true,\
       IgnorePublicAcls=true,\
       BlockPublicPolicy=true,\
       RestrictPublicBuckets=true
   ```

5. **Enable access logging**:
   ```bash
   aws s3api put-bucket-logging \
     --bucket your-notion-backups \
     --bucket-logging-status '{
       "LoggingEnabled": {
         "TargetBucket": "your-logging-bucket",
         "TargetPrefix": "notion-backup-logs/"
       }
     }'
   ```

## Advanced Usage

### Custom S3 Endpoint (MinIO, etc.)

```bash
# Set custom endpoint
export AWS_ENDPOINT_URL=https://minio.example.com
python backup_to_s3.py
```

### Cross-Region Replication

For disaster recovery, replicate backups to another region:

```bash
aws s3api put-bucket-replication \
  --bucket your-notion-backups \
  --replication-configuration file://replication.json
```

### Backup Retention Policy

Customize how long backups are kept by modifying the index management in the scripts.

## Support

For issues or questions:
1. Check the troubleshooting section above
2. Review AWS CloudWatch logs
3. Enable debug mode: `python backup_to_s3.py --debug`
4. Check the GitHub repository issues

## Related Documentation

- [AWS IAM Roles for Service Accounts (IRSA)](https://docs.aws.amazon.com/eks/latest/userguide/iam-roles-for-service-accounts.html)
- [AWS S3 Best Practices](https://docs.aws.amazon.com/AmazonS3/latest/userguide/best-practices.html)
- [Boto3 S3 Documentation](https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html)

