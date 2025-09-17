#!/usr/bin/env python3
"""
Backup validation script.

This script validates existing backups for compatibility and provides
detailed reports on potential restoration issues.
"""

import sys
import json
from pathlib import Path
from typing import Dict, Any

# Add src to Python path to allow imports
src_path = Path(__file__).parent / "src"
sys.path.insert(0, str(src_path))

from src.notion_backup_restore.backup.backup_processor import BackupProcessor
from src.notion_backup_restore.utils.logger import setup_logger


def validate_backup(backup_dir: Path) -> Dict[str, Any]:
    """
    Validate a backup directory for compatibility.
    
    Args:
        backup_dir: Path to backup directory
        
    Returns:
        Validation results
    """
    logger = setup_logger("backup_validator", verbose=True)
    processor = BackupProcessor(logger)
    
    logger.info(f"Validating backup: {backup_dir}")
    
    # Validate backup compatibility
    results = processor.validate_backup_compatibility(backup_dir)
    
    return results


def print_validation_results(results: Dict[str, Any]) -> None:
    """Print validation results in a human-readable format."""
    print("\n" + "="*60)
    print("BACKUP VALIDATION REPORT")
    print("="*60)
    
    print(f"Backup Directory: {results['backup_dir']}")
    print(f"Validated At: {results['validated_at']}")
    print(f"Compatible: {'✅ YES' if results['is_compatible'] else '❌ NO'}")
    
    if results['issues']:
        print(f"\n🚨 ISSUES FOUND ({len(results['issues'])}):")
        for i, issue in enumerate(results['issues'], 1):
            print(f"  {i}. {issue}")
    
    if results['warnings']:
        print(f"\n⚠️  WARNINGS ({len(results['warnings'])}):")
        for i, warning in enumerate(results['warnings'], 1):
            print(f"  {i}. {warning}")
    
    if results['databases']:
        print(f"\n📊 DATABASE VALIDATION:")
        for db_name, db_results in results['databases'].items():
            status = "✅" if db_results['is_valid'] else "❌"
            print(f"  {status} {db_name}")
            
            if db_results.get('issues'):
                for issue in db_results['issues']:
                    print(f"    - ❌ {issue}")
            
            if db_results.get('warnings'):
                for warning in db_results['warnings']:
                    print(f"    - ⚠️  {warning}")
    
    print("\n" + "="*60)
    
    if results['is_compatible']:
        print("✅ This backup should restore successfully!")
    else:
        print("❌ This backup may encounter errors during restoration.")
        print("   Consider reprocessing with the enhanced backup system.")


def main():
    """Main validation script."""
    if len(sys.argv) != 2:
        print("Usage: python validate_backup.py <backup_directory>")
        print("\nExample:")
        print("  python validate_backup.py backups/backup_20250916_095206")
        sys.exit(1)
    
    backup_dir = Path(sys.argv[1])
    
    if not backup_dir.exists():
        print(f"Error: Backup directory does not exist: {backup_dir}")
        sys.exit(1)
    
    if not backup_dir.is_dir():
        print(f"Error: Path is not a directory: {backup_dir}")
        sys.exit(1)
    
    try:
        # Validate the backup
        results = validate_backup(backup_dir)
        
        # Print results
        print_validation_results(results)
        
        # Save detailed results to file
        results_file = backup_dir / "validation_results.json"
        with open(results_file, 'w', encoding='utf-8') as f:
            json.dump(results, f, indent=2, ensure_ascii=False, default=str)
        
        print(f"\n📄 Detailed results saved to: {results_file}")
        
        # Exit with appropriate code
        sys.exit(0 if results['is_compatible'] else 1)
        
    except Exception as e:
        print(f"Error during validation: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
