# Notion Backup & Restore System

A production-ready Notion backup and restore system specifically designed for complex workspace structures with relationships, formulas, and custom properties.

## Features

- **Complete Schema Preservation**: Backs up and restores all property types, configurations, and relationships
- **Enhanced Compatibility Processing**: Automatically normalizes backup data to prevent restoration errors
- **Phased Restoration**: 4-phase approach ensures proper dependency handling and relationship integrity
- **Rate Limiting**: Respects Notion API limits (3 req/s) with sophisticated rate limiting and retry logic
- **Relationship Mapping**: Intelligent ID remapping system preserves relationships during restoration
- **Formula Support**: Preserves complex formulas including ROI calculations and rollup properties
- **Validation**: Comprehensive integrity checking for both backup and restore operations
- **Progress Tracking**: Real-time progress updates and detailed operation reports
- **API Version Compatibility**: Handles API changes automatically to prevent restoration failures

## Installation

1. Clone this repository:
```bash
git clone <repository-url>
cd notion-backup-restore
```

2. Install dependencies:
```bash
pip install -e .
```

3. For development:
```bash
pip install -e ".[dev]"
```

## Configuration

1. Copy the environment template:
```bash
cp env.example .env
```

2. Configure your Notion integration:
   - Go to [Notion Integrations](https://www.notion.so/my-integrations)
   - Create a new integration
   - Copy the "Internal Integration Token"
   - Add it to your `.env` file as `NOTION_TOKEN`

3. Share your databases with the integration:
   - Open each database and wiki (Documentation, Tasks, Notes, Sprints)
   - Click "Share" → "Invite" → Select your integration
   - Grant "Edit" permissions
   - Note: Documentation is a wiki but still needs to be shared for backup

## Workspace Structure

This system is designed for workspaces with the following structure:

```
Teamspace: General
└── Page: Databases
    ├── Documentation (Wiki - backed up like a database)
    │   ├── Title (Title)
    │   ├── Category (Select)
    │   ├── Status (Select)
    │   ├── Priority (Select)
    │   ├── Tags (Multi-select)
    │   ├── Related Tasks (Relation → Tasks)
    │   ├── Created (Created time)
    │   ├── Last Edited (Last edited time)
    │   └── Assignee (Person)
    │
    ├── Tasks (Database)
    │   ├── Task Name (Title)
    │   ├── Status (Select)
    │   ├── Priority (Select)
    │   ├── Assignee (Person)
    │   ├── Due Date (Date)
    │   ├── Effort (Number)
    │   ├── Value (Number)
    │   ├── ROI (Formula: round(Value/(Effort*400)*10)/10)
    │   ├── Documentation (Relation → Documentation)
    │   ├── Notes (Relation → Notes)
    │   ├── Sprint (Relation → Sprints)
    │   ├── Created (Created time)
    │   └── Last Edited (Last edited time)
    │
    ├── Notes (Database)
    │   ├── Title (Title)
    │   ├── Category (Select)
    │   ├── Tags (Multi-select)
    │   ├── Related Task (Relation → Tasks)
    │   ├── Created (Created time)
    │   ├── Last Edited (Last edited time)
    │   └── Author (Person)
    │
    └── Sprints (Database)
        ├── Sprint Name (Title)
        ├── Status (Select)
        ├── Start Date (Date)
        ├── End Date (Date)
        ├── Tasks (Relation → Tasks)
        ├── Created (Created time)
        └── Last Edited (Last edited time)
```

**Note**: Documentation is technically a wiki, but it's backed up and restored like a database since it has the same structure and relationships. All four items (Documentation, Tasks, Notes, Sprints) are included in backup/restore operations.

## Enhanced Backup System

This system now includes automatic data processing to prevent restoration errors caused by Notion API changes. The enhanced backup system:

### Key Improvements

1. **User Object Normalization**: Removes deprecated fields (`name`, `avatar_url`, `type`, `person`) that cause validation errors
2. **Relation Configuration Fixes**: Ensures all relation properties have proper `single_property` or `dual_property` configuration
3. **Content Block Sanitization**: Validates and truncates oversized content, removes invalid blocks
4. **Select Option Validation**: Cleans corrupted select/multi-select options
5. **API Version Compatibility**: Stores data in format compatible with current API requirements

### Enhanced Tools

- `validate_backup.py` - Check existing backups for compatibility issues
- `process_existing_backup.py` - Convert old backups to compatible format
- `test_backup_limited.py` - Create test backups with page limits for faster testing
- `test_enhanced_backup.py` - Test the processing system
- `test_content_block_validation.py` - Test content block validation features

### Configuration

Set `BACKUP_PROCESS_FOR_COMPATIBILITY=true` in your `.env` file (default: enabled)

### Important Limitations

**Database View Blocks**: While database view blocks can be backed up, they **cannot be automatically restored** due to Notion API limitations. The view configuration is preserved in backup files for reference, but views must be manually recreated after restore. See `DATABASE_VIEW_LIMITATIONS.md` for detailed information and workarounds.

## Usage

### Backup

Create a complete backup of your workspace:

```bash
python backup.py
```

Options:
- `--output-dir`: Specify backup directory (default: ./backups)
- `--include-blocks`: Include page block content (default: false)
- `--validate`: Run integrity validation after backup (default: true)
- `--verbose`: Enable verbose logging

Example:
```bash
python backup.py --output-dir ./my-backups --include-blocks --verbose
```

### Restore

Restore from a backup:

```bash
python restore.py --backup-dir ./backups/backup_20231215_143022
```

Options:
- `--backup-dir`: Path to backup directory (required)
- `--parent-id`: Parent page ID for restored databases
- `--dry-run`: Preview changes without executing (default: false)
- `--validate`: Run integrity validation after restore (default: true)
- `--verbose`: Enable verbose logging

Example:
```bash
python restore.py --backup-dir ./backups/backup_20231215_143022 --parent-id abc123 --dry-run
```

### Validation

Validate workspace structure before backup:

```bash
python scripts/validate_workspace.py
```

Set up and test Notion integration:

```bash
python scripts/setup_integration.py
```

## Backup Structure

Backups are stored in timestamped directories with the following structure:

```
backups/
└── backup_20231215_143022/
    ├── manifest.json           # Backup metadata and validation info
    ├── databases/
    │   ├── documentation_schema.json
    │   ├── documentation_data.json
    │   ├── tasks_schema.json
    │   ├── tasks_data.json
    │   ├── notes_schema.json
    │   ├── notes_data.json
    │   ├── sprints_schema.json
    │   └── sprints_data.json
    └── logs/
        └── backup.log
```

## Restoration Process

The restoration follows a 4-phase approach:

1. **Phase 1 - Database Creation**: Create databases with basic properties (text, number, select, etc.)
2. **Phase 2 - Relation Properties**: Add relation properties with updated database IDs
3. **Phase 3 - Formula Properties**: Add formula and rollup properties
4. **Phase 4 - Data Population**: Create pages and populate with data

This phased approach ensures proper dependency handling and relationship integrity.

## Rate Limiting

The system respects Notion's API rate limits:
- **Average**: 3 requests per second
- **Burst**: Up to 5 requests in short bursts
- **Retry Logic**: Exponential backoff with jitter
- **Circuit Breaker**: Automatic failure detection and recovery

## Configuration Options

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `NOTION_TOKEN` | - | Notion integration token (required) |
| `BACKUP_OUTPUT_DIR` | `./backups` | Default backup directory |
| `RATE_LIMIT_REQUESTS_PER_SECOND` | `2.5` | API rate limit |
| `MAX_RETRIES` | `3` | Maximum retry attempts |
| `LOG_LEVEL` | `INFO` | Logging level |
| `VALIDATION_TIMEOUT` | `300` | Validation timeout (seconds) |

See `env.example` for complete configuration options.

## Troubleshooting

### Common Issues

1. **"Database not found"**
   - Ensure databases are shared with your integration
   - Verify database names match exactly (case-sensitive)
   - Check integration has proper permissions

2. **Rate limiting errors**
   - Reduce `RATE_LIMIT_REQUESTS_PER_SECOND` in `.env`
   - Increase `RETRY_MAX_DELAY` for longer backoff

3. **Relationship restoration failures**
   - Ensure all related databases are included in backup
   - Check dependency order in restoration logs
   - Verify relation properties are properly configured

4. **Formula property errors**
   - Check formula syntax in original database
   - Ensure referenced properties exist before formula creation
   - Review formula restoration logs for specific errors

### Debug Mode

Enable debug logging:

```bash
export DEBUG=true
export LOG_LEVEL=DEBUG
python backup.py --verbose
```

### Validation Reports

After backup or restore, check the validation report:

```
backups/backup_20231215_143022/validation_report.json
```

This includes:
- Schema validation results
- Data integrity checks
- Relationship validation
- Formula verification
- Row count comparisons

## API Rate Limiting

The Notion API has the following limits:
- **Rate**: 3 requests per second (average)
- **Burst**: Short bursts allowed
- **Retry-After**: Respect retry-after headers

Our implementation:
- Uses sliding window rate limiting
- Implements exponential backoff with jitter
- Includes circuit breaker pattern
- Provides detailed rate limiting logs

## Development

### Running Tests

```bash
# Run all tests
pytest

# Run with coverage
pytest --cov=src

# Run specific test file
pytest tests/test_backup.py

# Run integration tests
pytest -m integration
```

### Code Quality

```bash
# Format code
black src tests

# Lint code
ruff src tests

# Type checking
mypy src
```

### Pre-commit Hooks

```bash
pre-commit install
pre-commit run --all-files
```

## License

MIT License - see LICENSE file for details.

## Support

For issues and questions:
1. Check the troubleshooting section
2. Review validation reports
3. Enable debug logging
4. Check Notion API status
