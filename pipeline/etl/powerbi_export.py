"""
POWER BI CONNECTOR - Export/sync data mart to Power BI

Purpose:
- Provide multiple methods to connect DuckDB data mart to Power BI
- Option 1: Direct DuckDB connection (Power BI Premium or on-premise gateway)
- Option 2: Export to Parquet files (Power BI Online)
- Option 3: Export to CSV files (universal compatibility)
- Option 4: Push to data lakehouse (if using Power BI Premium with Fabric)

Workflow:
1. Read from DuckDB database: data/data mart/weather.duckdb
2. Export tables in various formats based on user preference
3. Provide connection strings and configuration for Power BI
4. Log file locations for easy import

Requirements:
- duckdb, pandas libraries
- Power BI Desktop or Power BI Service with access methods configured
"""

import os
import logging
from pathlib import Path
from datetime import datetime
import json

try:
    import duckdb
    import pandas as pd
except ImportError as e:
    print(f"Error: Missing required libraries: {e}")
    print("Install with: pip install duckdb pandas")
    exit(1)

# =============================
# LOGGING SETUP
# =============================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('pipeline.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger('powerbi_export')

# =============================
# CONFIG
# =============================
script_dir = Path(__file__).parent
project_root = script_dir.parent

DATA_MART_DIR = project_root / "data" / "data mart"
EXPORTS_DIR = DATA_MART_DIR / "powerbi_exports"
DUCKDB_FILE = DATA_MART_DIR / "weather.duckdb"

EXPORTS_DIR.mkdir(parents=True, exist_ok=True)


class PowerBIConnector:
    """Manage exports and connections to Power BI"""
    
    def __init__(self, duckdb_path: Path, export_dir: Path):
        self.duckdb_path = duckdb_path
        self.export_dir = export_dir
        self.conn = None
        
    def connect(self):
        """Connect to DuckDB"""
        logger.info(f"Connecting to DuckDB: {self.duckdb_path}")
        self.conn = duckdb.connect(str(self.duckdb_path), read_only=True)
        return self.conn
    
    def close(self):
        """Close connection"""
        if self.conn:
            self.conn.close()
    
    def export_to_parquet(self) -> bool:
        """
        Export all tables and views to Parquet format
        
        Best for: Power BI Online, Power BI Fabric
        Advantages:
        - Compressed format (smaller file size)
        - Preserves data types
        - Good performance
        """
        try:
            logger.info("Exporting tables to Parquet format...")
            
            parquet_dir = self.export_dir / "parquet" / datetime.now().strftime("%Y-%m-%d")
            parquet_dir.mkdir(parents=True, exist_ok=True)
            
            # Tables and views to export
            objects = [
                ('location_dim', 'Dimension table: locations'),
                ('date_dim', 'Dimension table: dates'),
                ('weather_daily_facts', 'Fact table: daily weather facts'),
                ('weather_monthly_summary', 'View: monthly aggregated summary'),
                ('temperature_trends', 'View: temperature trends with 30-day MA'),
                ('rainfall_patterns', 'View: monthly rainfall patterns'),
            ]
            
            exported_files = []
            for obj_name, description in objects:
                try:
                    output_file = parquet_dir / f"{obj_name}.parquet"
                    self.conn.execute(f"COPY {obj_name} TO '{output_file}' (FORMAT PARQUET)")
                    
                    file_size = output_file.stat().st_size / (1024 * 1024)
                    logger.info(f"  ✓ {obj_name}: {file_size:.2f} MB")
                    exported_files.append({
                        'name': obj_name,
                        'description': description,
                        'file': str(output_file),
                        'size_mb': round(file_size, 2)
                    })
                except Exception as e:
                    logger.warning(f"  ✗ Failed to export {obj_name}: {e}")
            
            self._write_export_manifest('parquet', exported_files)
            return True
            
        except Exception as e:
            logger.error(f"✗ Failed to export Parquet: {e}")
            return False
    
    def export_to_csv(self) -> bool:
        """
        Export all tables to CSV format
        
        Best for: Universal compatibility, Excel, Power BI Desktop
        Advantages:
        - Works everywhere
        - Human-readable
        - Easy for manual data inspection
        """
        try:
            logger.info("Exporting tables to CSV format...")
            
            csv_dir = self.export_dir / "csv" / datetime.now().strftime("%Y-%m-%d")
            csv_dir.mkdir(parents=True, exist_ok=True)
            
            objects = [
                ('location_dim', 'Dimension table: locations'),
                ('date_dim', 'Dimension table: dates'),
                ('weather_daily_facts', 'Fact table: daily weather facts'),
                ('weather_monthly_summary', 'View: monthly aggregated summary'),
                ('temperature_trends', 'View: temperature trends with 30-day MA'),
                ('rainfall_patterns', 'View: monthly rainfall patterns'),
            ]
            
            exported_files = []
            for obj_name, description in objects:
                try:
                    output_file = csv_dir / f"{obj_name}.csv"
                    self.conn.execute(f"""
                        COPY {obj_name} TO '{output_file}' 
                        (FORMAT CSV, HEADER TRUE, ENCODING 'UTF8')
                    """)
                    
                    file_size = output_file.stat().st_size / (1024 * 1024)
                    logger.info(f"  ✓ {obj_name}: {file_size:.2f} MB")
                    exported_files.append({
                        'name': obj_name,
                        'description': description,
                        'file': str(output_file),
                        'size_mb': round(file_size, 2)
                    })
                except Exception as e:
                    logger.warning(f"  ✗ Failed to export {obj_name}: {e}")
            
            self._write_export_manifest('csv', exported_files)
            return True
            
        except Exception as e:
            logger.error(f"✗ Failed to export CSV: {e}")
            return False
    
    def export_fact_table_only(self, format: str = 'parquet') -> bool:
        """
        Export only the main fact table for incremental updates
        
        Useful for scheduling regular syncs without full exports
        """
        try:
            logger.info(f"Exporting fact table in {format} format...")
            
            format_dir = self.export_dir / format / datetime.now().strftime("%Y-%m-%d")
            format_dir.mkdir(parents=True, exist_ok=True)
            
            if format == 'parquet':
                output_file = format_dir / "weather_daily_facts.parquet"
                self.conn.execute(f"COPY weather_daily_facts TO '{output_file}' (FORMAT PARQUET)")
            else:  # csv
                output_file = format_dir / "weather_daily_facts.csv"
                self.conn.execute(f"""
                    COPY weather_daily_facts TO '{output_file}' 
                    (FORMAT CSV, HEADER TRUE, ENCODING 'UTF8')
                """)
            
            file_size = output_file.stat().st_size / (1024 * 1024)
            logger.info(f"  ✓ Fact table exported: {file_size:.2f} MB")
            return True
            
        except Exception as e:
            logger.error(f"✗ Failed to export fact table: {e}")
            return False
    
    def generate_powerbi_config(self):
        """Generate Power BI configuration guide"""
        config_path = self.export_dir / "POWERBI_SETUP.md"
        
        content = f"""# Power BI Setup Guide

## Database Information
- **Database Type**: DuckDB
- **Location**: {self.duckdb_path}
- **Last Updated**: {datetime.now().isoformat()}

## Connection Methods

### Method 1: Direct DuckDB Connection (Recommended)
**Requirements**: Power BI Desktop (Enterprise Edition) or on-premise gateway

**Steps**:
1. In Power BI Desktop, click "Get Data"
2. Search for "DuckDB" or use ODBC connector
3. Connection string:
   ```
   {self.duckdb_path}
   ```
4. Select tables:
   - weather_daily_facts (main fact table)
   - location_dim (provinces/locations)
   - date_dim (dates and calendar info)
5. Create relationships:
   - weather_daily_facts.date_key → date_dim.date_key
   - weather_daily_facts.province_name → location_dim.province_name

### Method 2: Parquet Files (Power BI Online Compatible)
**Requirements**: Power BI Desktop or Online

**Steps**:
1. Download Parquet files from: {self.export_dir}/parquet/
2. In Power BI, click "Get Data" → "Parquet"
3. Select exported .parquet files
4. Import and create relationships as in Method 1

### Method 3: CSV Files (Universal)
**Requirements**: Any version of Power BI

**Steps**:
1. Download CSV files from: {self.export_dir}/csv/
2. In Power BI, click "Get Data" → "CSV"
3. Select exported .csv files
4. Ensure correct data type mappings
5. Create relationships as in Method 1

## Table Structure

### Dimensions
- **location_dim**: Province information
  - location_id (primary key)
  - province_name

- **date_dim**: Date information for time-based analysis
  - date_key (primary key)
  - year, month, day, quarter, week_number, day_of_week

### Facts
- **weather_daily_facts**: Daily aggregated weather data
  - date_key (foreign key to date_dim)
  - province_name (foreign key to location_dim)
  - avg_temp_c, max_temp_c, min_temp_c
  - avg_humidity_pct
  - total_precipitation_mm
  - avg_wind_speed, max_gust_speed
  - avg_cloud_cover_pct
  - weather_code
  - record_count

### Views (Summary Tables)
- **weather_monthly_summary**: Monthly aggregated data by province
- **temperature_trends**: Temperature trends with 30-day moving average
- **rainfall_patterns**: Monthly rainfall statistics

## Suggested Visuals

1. **Map**: Temperature distribution across provinces
2. **Line Chart**: Temperature trends over time (monthly)
3. **Column Chart**: Monthly precipitation by province
4. **Gauge**: Current (latest) average temperature by province
5. **Matrix/Table**: Weather monthly summary

## Refresh Schedule

Recommended refresh frequency:
- Daily: weather_daily_facts (new daily data)
- Weekly: monthly_summary and trend views
- Manual: After significant data corrections

## Troubleshooting

- **DuckDB Connection Error**: Ensure DuckDB ODBC driver is installed
- **File Not Found**: Verify export directory and file locations
- **Performance Issues**: Use filtered imports or create aggregated views
- **Data Type Issues**: Check CSV export encoding (UTF-8)

## Next Steps

1. Test connection with sample visuals
2. Create measures for common calculations:
   - Average Temperature = AVERAGE(weather_daily_facts[avg_temp_c])
   - Total Precipitation = SUM(weather_daily_facts[total_precipitation_mm])
   - Days with Rain = COUNTA(weather_daily_facts[total_precipitation_mm > 0])
3. Set up automatic refresh schedule
4. Share dashboard with stakeholders

"""
        
        with open(config_path, 'w', encoding='utf-8') as f:
            f.write(content)
        
        logger.info(f"✓ Generated Power BI setup guide: {config_path}")
        return config_path
    
    def _write_export_manifest(self, format: str, files: list):
        """Write manifest of exported files"""
        manifest = {
            'format': format,
            'export_date': datetime.now().isoformat(),
            'duckdb_file': str(self.duckdb_path),
            'files': files,
            'total_size_mb': sum(f.get('size_mb', 0) for f in files)
        }
        
        manifest_path = self.export_dir / f"manifest_{format}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        with open(manifest_path, 'w', encoding='utf-8') as f:
            json.dump(manifest, f, indent=2, default=str)
        
        logger.info(f"  Manifest: {manifest_path}")


def main():
    """Main entry point"""
    logger.info("="*60)
    logger.info("POWER BI EXPORT - Prepare data for Power BI")
    logger.info("="*60)
    
    if not DUCKDB_FILE.exists():
        logger.error(f"DuckDB file not found: {DUCKDB_FILE}")
        logger.error("Run duckdb_datamart.py first")
        return 1
    
    connector = PowerBIConnector(DUCKDB_FILE, EXPORTS_DIR)
    
    try:
        connector.connect()
        
        # Export in multiple formats for flexibility
        parquet_success = connector.export_to_parquet()
        csv_success = connector.export_to_csv()
        
        # Generate setup guide
        config_path = connector.generate_powerbi_config()
        
        logger.info("="*60)
        logger.info("✅ Power BI export completed!")
        logger.info(f"   Parquet exports: {EXPORTS_DIR / 'parquet'}")
        logger.info(f"   CSV exports: {EXPORTS_DIR / 'csv'}")
        logger.info(f"   Setup guide: {config_path}")
        logger.info("   Next step: Open Power BI and connect to exported files")
        
        return 0 if (parquet_success and csv_success) else 1
        
    except Exception as e:
        logger.error(f"✗ Unexpected error: {e}")
        return 1
        
    finally:
        connector.close()


if __name__ == '__main__':
    import sys
    exit_code = main()
    sys.exit(exit_code)
