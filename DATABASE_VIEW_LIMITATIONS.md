# Database View Block Limitations

## Overview

Database view blocks can be **backed up** but have **significant restoration limitations** due to Notion API constraints. This document explains what works, what doesn't, and how to handle database views in your backup/restore workflow.

## What Gets Backed Up ✅

When you backup pages containing database view blocks, the system will preserve:

- **View Configuration**: Filters, sorts, grouping settings
- **Database Reference**: The ID of the source database
- **View Type**: Table, board, calendar, etc.
- **Property Settings**: Which columns are visible/hidden
- **Block Structure**: The view block's position in the page

## What Cannot Be Restored ❌

Due to Notion API limitations, the following **cannot be automatically restored**:

- **View Creation**: The API doesn't support creating database views
- **Filter Configuration**: Complex filters cannot be programmatically set
- **Sort Settings**: Custom sorting cannot be restored
- **Grouping**: Board groupings and other view-specific settings
- **Column Widths**: Visual formatting of table views
- **View Names**: Custom view names are not preserved

## Current Behavior

### During Backup
```
✅ Database view blocks are detected and backed up
⚠️  Warning logged: "Database view block detected - can be backed up but view configuration cannot be restored due to Notion API limitations"
✅ View configuration preserved in backup files for reference
```

### During Restore
```
❌ Database view blocks are skipped during restoration
📝 View configuration available in backup files for manual recreation
```

## Workaround Strategy

### 1. Manual Documentation
Before backup, document your database views:
- Screenshot each view configuration
- Note filter conditions and sort orders
- Record grouping settings and column arrangements

### 2. Backup Reference
The backup files contain the view configuration in JSON format:
```json
{
  "type": "database_view",
  "database_view": {
    "database_id": "12345678-1234-1234-1234-123456789abc",
    "view_type": "table",
    "filters": [
      {
        "property": "Status",
        "condition": "equals", 
        "value": "Done"
      }
    ],
    "sorts": [
      {
        "property": "Created",
        "direction": "descending"
      }
    ]
  }
}
```

### 3. Manual Recreation
After restore:
1. Navigate to the restored database
2. Create new views manually
3. Use the backup JSON as reference for filters/sorts
4. Recreate grouping and formatting settings

## Alternative Solutions

### 1. Inline Database References
Instead of database view blocks, consider:
- **Linked Database Pages**: Create dedicated pages for each view
- **Database Links**: Use simple database links that can be restored
- **Manual Views**: Create views directly in the database rather than as blocks

### 2. Template Approach
- Create template pages with common view configurations
- Document view settings in the template
- Use templates to quickly recreate views after restore

## Technical Details

### API Limitations
The Notion API currently doesn't support:
- `POST /databases/{database_id}/views` (doesn't exist)
- Creating views programmatically
- Setting view filters/sorts via API
- Configuring view properties

### Future Improvements
If Notion adds view creation to their API, this limitation could be resolved in future versions of the backup system.

## Best Practices

1. **Document Critical Views**: Screenshot and document your most important database views
2. **Minimize View Complexity**: Use simpler views that are easier to recreate
3. **Regular Backups**: Backup frequently so view recreation is based on recent configurations
4. **Test Restores**: Practice restoring and recreating views in a test workspace

## Summary

While database view blocks **can be backed up**, they **cannot be automatically restored** due to Notion API limitations. The backup system preserves the view configuration for reference, but manual recreation is required after restore.

This is a known limitation of the Notion API, not a limitation of the backup system itself.
