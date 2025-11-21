# Notion API Changes and Compatibility

## Recent API Changes (2024-2025)

### Database Search Filter Change

**Date**: November 2024 - January 2025

**Issue**: The Notion API changed how databases are searched and filtered.

**Old Behavior** (Before):
```python
search_results = api_client.search(
    query="Database Name",
    filter={
        "value": "database",
        "property": "object"
    }
)
```

**Error Message**:
```
body.filter.value should be "page" or "data_source", instead was "database"
```

**New Behavior** (Current):
According to Notion API documentation:
- Databases are now referred to as "Data Sources" in the API
- Valid filter values are: `"page"` or `"data_source"` (not `"database"`)
- Databases still return as `object: "database"` in results

**Our Solution**:
```python
# Search without filter - databases appear in general results
search_results = api_client.search(
    query="Database Name"
)

# Then filter results by object type
for result in search_results.get("results", []):
    if result.get("object") == "database":
        # This is a database
        ...
```

### Why This Works

1. **Search without filter** returns all matching objects (pages, databases, etc.)
2. **Databases still have** `"object": "database"` in the response
3. **We filter client-side** by checking the object type
4. **More reliable** - works regardless of API filter changes

### Alternative Approaches (Not Recommended)

You could try using the new filter values:

```python
# Option 1: Filter by "page" (may include databases)
search_results = api_client.search(
    query="Database Name",
    filter={
        "value": "page",
        "property": "object"
    }
)

# Option 2: Filter by "data_source" (untested)
search_results = api_client.search(
    query="Database Name",
    filter={
        "value": "data_source",
        "property": "object"
    }
)
```

However, these approaches are less reliable because:
- Notion's terminology is inconsistent (databases vs data sources)
- Filter behavior may change again
- No filter works universally

## API Version Compatibility

### Current Implementation

Our implementation is designed to be **version-agnostic**:

- ✅ Works with current Notion API (2024-2025)
- ✅ No hardcoded API version dependencies
- ✅ Graceful handling of API changes
- ✅ Client-side filtering for reliability

### Notion API Versioning

Notion uses API versioning in headers:
```
Notion-Version: 2022-06-28
```

Our SDK (notion-client) handles this automatically, but we should be aware of:
- Property type changes
- New property types
- Deprecated fields
- Response structure changes

## Known API Limitations

### 1. Database View Blocks

**Issue**: Database view blocks cannot be restored via API

**Workaround**: 
- Backup preserves view configuration
- Manual recreation required after restore
- See `DATABASE_VIEW_LIMITATIONS.md` for details

### 2. Rate Limiting

**Current Limits**:
- 3 requests per second (average)
- Burst allowance: ~5 requests

**Our Implementation**:
- Rate limiting: 2.5 req/s (safe margin)
- Automatic backoff and retry
- Exponential backoff on errors

### 3. Search API Limitations

**Limitations**:
- No exact match filter
- Case-insensitive search
- Partial matches included
- Limited to accessible content

**Our Solution**:
- Search by name
- Filter exact matches client-side
- Validate database structure
- Handle partial matches gracefully

## Compatibility Testing

### Test Checklist

When Notion API changes:

- [ ] Database discovery (search)
- [ ] Schema extraction (get_database)
- [ ] Content extraction (query_database)
- [ ] Page creation (create_page)
- [ ] Property updates (update_page)
- [ ] Block content (get_block_children)
- [ ] Relation properties
- [ ] Formula properties
- [ ] Rollup properties

### Quick Test Command

```bash
# Test database discovery
python backup.py main --verbose --no-validate

# Test full backup
python backup.py main --include-blocks --verbose

# Test restore
python restore.py main ./backups/latest --dry-run --verbose
```

## Monitoring API Changes

### Resources

- **Official Docs**: https://developers.notion.com/
- **API Reference**: https://developers.notion.com/reference/intro
- **Changelog**: Check Notion's developer changelog
- **Community**: Notion Devs Slack community

### Signs of API Changes

Watch for:
- ❌ Sudden search failures
- ❌ Property type errors
- ❌ Validation errors
- ❌ Unexpected response structures
- ⚠️ Deprecation warnings in logs

## Migration Guide

### If API Changes Break Compatibility

1. **Identify the Issue**
   ```bash
   python backup.py main --debug
   # Check logs for API errors
   ```

2. **Check Notion Docs**
   - Review API changelog
   - Check for version updates
   - Look for migration guides

3. **Update Code**
   - Modify affected modules
   - Update tests
   - Test thoroughly

4. **Update Dependencies**
   ```bash
   pip install --upgrade notion-client
   ```

5. **Document Changes**
   - Update this file
   - Add migration notes
   - Update README if needed

## Best Practices

### 1. Defensive Programming

```python
# Always check for field existence
title = result.get("title", [])
if title:
    name = "".join([t.get("plain_text", "") for t in title])

# Handle missing properties gracefully
properties = database.get("properties", {})
for prop_name, prop_data in properties.items():
    prop_type = prop_data.get("type", "unknown")
```

### 2. Version-Agnostic Code

```python
# Don't hardcode field names that might change
# Use configuration for expected structure
from config import WORKSPACE_DATABASES

expected_props = WORKSPACE_DATABASES[db_name]["properties"]
```

### 3. Comprehensive Logging

```python
# Log API responses for debugging
self.logger.debug(f"API response: {json.dumps(response, indent=2)}")

# Log version information
self.logger.info(f"Using notion-client version: {notion.__version__}")
```

### 4. Graceful Degradation

```python
# Continue with warnings instead of failing
try:
    optional_data = extract_optional_field(result)
except Exception as e:
    self.logger.warning(f"Could not extract optional field: {e}")
    optional_data = None
```

## Related Files

- `src/notion_backup_restore/backup/database_finder.py` - Database search implementation
- `src/notion_backup_restore/utils/api_client.py` - API client wrapper
- `DATABASE_VIEW_LIMITATIONS.md` - Known limitations
- `README.md` - Main documentation

## Support

If you encounter API compatibility issues:

1. Check this document first
2. Review error logs with `--debug` flag
3. Check Notion's developer documentation
4. Open an issue on GitHub with:
   - Error message
   - API response (if available)
   - notion-client version
   - Steps to reproduce

## Version History

| Date | Change | Impact |
|------|--------|--------|
| 2024-11 | Database filter deprecated | High - Fixed by removing filter |
| 2024-06 | API versioning introduced | Low - SDK handles automatically |
| 2023-12 | Property types expanded | Medium - Added new type handlers |

---

Last Updated: November 2024

