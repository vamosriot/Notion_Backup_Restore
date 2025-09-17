# Project Structure

## 📁 **Clean Project Layout**

```
Notion_Backup_Restore/
├── 🚀 Core Scripts
│   ├── backup.py                    # Main backup entry point
│   ├── restore.py                   # Main restore entry point
│   ├── validate_backup.py           # Backup compatibility validator
│   └── process_existing_backup.py   # Convert old backups to compatible format
│
├── 🧪 Testing & Development
│   ├── test_backup_limited.py       # Create test backups with page limits
│   ├── test_enhanced_backup.py      # Test enhanced backup features
│   └── test_content_block_validation.py # Test content block validation
│
├── 📚 Documentation
│   ├── README.md                    # Main project documentation
│   ├── DATABASE_VIEW_LIMITATIONS.md # Database view block limitations
│   ├── ENHANCED_BACKUP_SUMMARY.md   # Enhanced backup system overview
│   └── PROJECT_STRUCTURE.md         # This file
│
├── ⚙️ Configuration
│   ├── env.example                  # Environment variables template
│   ├── pyproject.toml              # Python project configuration
│   └── notion_backup_restore.log   # Application logs
│
├── 🏗️ Core System (src/)
│   └── notion_backup_restore/
│       ├── backup/                  # Backup system
│       │   ├── manager.py           # Main backup orchestration
│       │   ├── database_finder.py   # Database discovery
│       │   ├── schema_extractor.py  # Schema extraction
│       │   ├── content_extractor.py # Content extraction
│       │   ├── data_processor.py    # Data normalization
│       │   ├── backup_processor.py  # Backup processing orchestration
│       │   └── content_block_validator.py # Content block validation
│       │
│       ├── restore/                 # Restore system
│       │   ├── manager.py           # Main restore orchestration
│       │   ├── database_creator.py  # Database creation
│       │   ├── data_restorer.py     # Data restoration
│       │   ├── relation_restorer.py # Relation handling
│       │   └── formula_restorer.py  # Formula restoration
│       │
│       ├── utils/                   # Utilities
│       │   ├── api_client.py        # Notion API client
│       │   ├── logger.py            # Logging utilities
│       │   ├── rate_limiter.py      # API rate limiting
│       │   ├── id_mapper.py         # ID mapping for restoration
│       │   └── dependency_resolver.py # Dependency resolution
│       │
│       ├── cli/                     # Command line interfaces
│       │   ├── backup_cli.py        # Backup CLI
│       │   └── restore_cli.py       # Restore CLI
│       │
│       ├── validation/              # Validation system
│       │   └── integrity_checker.py # Backup integrity validation
│       │
│       └── config.py                # Configuration management
│
├── 🧪 Tests
│   └── tests/
│       ├── test_backup.py           # Backup system tests
│       ├── test_restore.py          # Restore system tests
│       ├── test_utils.py            # Utility tests
│       └── fixtures/                # Test fixtures
│
├── 📦 Data
│   └── backups/                     # Backup storage directory
│       └── backup_20250916_095206/  # Example backup
│
└── 🛠️ Scripts & Tools
    ├── scripts/
    │   ├── setup_integration.py     # Notion integration setup
    │   └── validate_workspace.py    # Workspace validation
    │
    └── Shell Scripts
        ├── run-backup-background.sh  # Background backup execution
        ├── run-backup-compressed.sh  # Compressed backup creation
        ├── run-backup-nosleep.sh     # Backup without sleep prevention
        ├── setup-google-drive.sh     # Google Drive integration
        └── cloud-setup.md            # Cloud setup documentation
```

## 🗑️ **Removed Legacy Files**

The following outdated fix scripts have been removed as they're replaced by the enhanced system:

- ~~`comprehensive_block_fixer.py`~~ → Replaced by `content_block_validator.py`
- ~~`create_demo_backup.py`~~ → Replaced by `test_backup_limited.py`
- ~~`create_documentation_only.py`~~ → Replaced by `test_backup_limited.py`
- ~~`create_working_backup.py`~~ → Replaced by enhanced `backup.py`
- ~~`fix_people_properties.py`~~ → Replaced by `data_processor.py`
- ~~`fix_rich_content.py`~~ → Replaced by `content_block_validator.py`
- ~~`fix_standalone_backup.py`~~ → Replaced by `process_existing_backup.py`

## 🎯 **Key Components**

### **Enhanced Backup System**
- **Automatic Processing**: Data normalized during backup to prevent restoration errors
- **Content Validation**: Comprehensive block validation and sanitization
- **API Compatibility**: Ensures data meets current Notion API requirements
- **User Normalization**: Removes deprecated user object fields
- **Relation Fixes**: Adds proper relation configurations

### **Testing Tools**
- **Limited Backup**: Create test backups with page limits for faster testing
- **Validation**: Check backup compatibility before restoration
- **Processing**: Convert old backups to compatible format

### **Core Architecture**
- **Modular Design**: Separate components for backup, restore, validation
- **Rate Limiting**: Respects Notion API limits
- **Progress Tracking**: Real-time progress updates
- **Error Handling**: Comprehensive error handling and recovery

## 🚀 **Usage Workflow**

1. **Setup**: Configure `.env` file with Notion token
2. **Backup**: `python backup.py` (creates processed, compatible backup)
3. **Validate**: `python validate_backup.py [backup_path]` (check compatibility)
4. **Restore**: `python restore.py [backup_path]` (clean restoration)

## 🧪 **Testing Workflow**

1. **Test Backup**: `python test_backup_limited.py 100` (create small test backup)
2. **Validate**: `python validate_backup.py [test_backup_path]` (verify compatibility)
3. **Test Restore**: `python restore.py [test_backup_path]` (test restoration)

The project structure is now clean, focused, and optimized for the enhanced backup/restore workflow! 🎉
