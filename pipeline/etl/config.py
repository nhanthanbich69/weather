"""
DUCKDB DATA MART - Process weather data into analytics-ready tables

Purpose:
- Download chunks from Supabase Storage or use local data.csv
- Create DuckDB database with dimensional model
- Build fact tables and summary views for analysis
- Optimize for Power BI consumption

Workflow:
1. Read data from:
   - Option A: chunks from Supabase Storage (if available)
   - Option B: local data.csv file (fallback)
2. Parse and clean data with proper type casting
3. Create dimensional model:
   - location_dim: unique provinces/locations
   - date_dim: calendar dimension
   - weather_daily_facts: main fact table
4. Create materialized views for common aggregations:
   - weather_monthly_summary
   - temperature_trends
   - rainfall_patterns
5. Build indices for performance

Requirements:
- duckdb, pandas libraries
- data/data lakehouse/data.csv OR chunks in Supabase
- ~5-10GB free space for DuckDB database
"""

import os
import sys
import logging
from pathlib import Path
from datetime import datetime, timedelta
import io
import json

try:
    import duckdb
    import pandas as pd
    import numpy as np
except ImportError as e:
    print(f"Error: Missing required libraries: {e}")
    print("Install with: pip install duckdb pandas numpy")
    sys.exit(1)

try:
    from supabase import create_client, Client
except ImportError:
    Client = None  # Supabase optional

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
logger = logging.getLogger('duckdb_datamart')

# =============================
# CONFIG
# =============================
script_dir = Path(__file__).parent
project_root = script_dir.parent

DATA_LAKEHOUSE_DIR = project_root / "data" / "data lakehouse"
DATA_MART_DIR = project_root / "data" / "data mart"
DATA_CSV_FILE = DATA_LAKEHOUSE_DIR / "data.csv"
DUCKDB_FILE = DATA_MART_DIR / "weather.duckdb"

DATA_MART_DIR.mkdir(parents=True, exist_ok=True)

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY") or os.getenv("SUPABASE_ANON_KEY")
SUPABASE_BUCKET = os.getenv("SUPABASE_BUCKET", "weather-data")


class DuckDBDataMart:
    """Build and manage DuckDB data mart for weather analytics"""
    
    def __init__(self, duckdb_path: Path):
        self.duckdb_path = duckdb_path
        self.conn = None
        
    def connect(self):
        """Connect to DuckDB, creating if necessary"""
        logger.info(f"Connecting to DuckDB: {self.duckdb_path}")
        self.conn = duckdb.connect(str(self.duckdb_path))
        return self.conn
    
    def close(self):
        """Close connection and save"""
        if self.conn:
            self.conn.close()
            logger.info("DuckDB connection closed")
    
    def load_data_from_csv(self, csv_file: Path) -> pd.DataFrame:
        """Load weather data from CSV file"""
        logger.info(f"Loading data from CSV: {csv_file}")
        
        if not csv_file.exists():
            raise FileNotFoundError(f"CSV file not found: {csv_file}")
        
        try:
            df = pd.read_csv(csv_file, encoding='utf-8-sig')
            logger.info(f"  Loaded {len(df)} rows, {len(df.columns)} columns")
            return df
        except Exception as e:
            logger.error(f"Failed to load CSV: {e}")
            raise
    
    def load_data_from_supabase(self) -> pd.DataFrame:
        """Load data from Supabase chunks (if available)"""
        if not Client or not SUPABASE_URL or not SUPABASE_KEY:
            logger.warning("Supabase not configured, falling back to CSV")
            return None
        
        try:
            logger.info("Attempting to load from Supabase Storage...")
            supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
            
            # List metadata files
            files = supabase.storage.from_(SUPABASE_BUCKET).list("weather_data")
            metadata_files = [f for f in files if f['name'].startswith('metadata_')]
            
            if not metadata_files:
                logger.warning("No metadata files in Supabase, falling back to CSV")
                return None
            
            # Get latest metadata
            latest_metadata = sorted(metadata_files, key=lambda x: x['name'], reverse=True)[0]
            metadata_path = f"weather_data/{latest_metadata['name']}"
            
            logger.info(f"Loading metadata: {metadata_path}")
            metadata_content = supabase.storage.from_(SUPABASE_BUCKET).download(metadata_path)
            metadata = json.loads(metadata_content)
            
            logger.info(f"  Found {metadata['total_chunks']} chunks ({metadata['total_rows']} rows)")
            
            # Download and concatenate chunks
            chunks = []
            for chunk_info in metadata['chunks']:
                chunk_file = chunk_info['filename']
                try:
                    logger.info(f"  Downloading: {chunk_file}")
                    chunk_data = supabase.storage.from_(SUPABASE_BUCKET).download(chunk_file)
                    chunk_df = pd.read_csv(io.BytesIO(chunk_data), encoding='utf-8-sig')
                    chunks.append(chunk_df)
                except Exception as e:
                    logger.warning(f"  Failed to download chunk {chunk_file}: {e}")
            
            if chunks:
                df = pd.concat(chunks, ignore_index=True)
                logger.info(f"✓ Loaded {len(df)} rows from {len(chunks)} chunks")
                return df
            else:
                logger.warning("No chunks loaded from Supabase")
                return None
                
        except Exception as e:
            logger.warning(f"Failed to load from Supabase: {e}")
            return None
    
    def load_data(self) -> pd.DataFrame:
        """Load data from best available source"""
        # Try Supabase first
        df = self.load_data_from_supabase()
        
        # Fallback to CSV
        if df is None or df.empty:
            df = self.load_data_from_csv(DATA_CSV_FILE)
        
        return df
    
    def clean_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean and type-cast data"""
        logger.info("Cleaning data...")
        
        # Ensure Datetime is datetime
        if 'Datetime' in df.columns:
            df['Datetime'] = pd.to_datetime(df['Datetime'], errors='coerce')
        
        # Remove rows with missing datetime
        initial_rows = len(df)
        df = df.dropna(subset=['Datetime'])
        logger.info(f"  Removed {initial_rows - len(df)} rows with invalid Datetime")
        
        # Fill NaN in numeric columns with 0
        numeric_cols = df.select_dtypes(include=[np.number]).columns
        for col in numeric_cols:
            df[col] = df[col].fillna(0)
        
        # Sort by province and datetime
        df = df.sort_values(['Tinh_thanh', 'Datetime']).reset_index(drop=True)
        
        logger.info(f"  Cleaned data: {len(df)} rows")
        return df
    
    def create_tables(self, df: pd.DataFrame):
        """Create fact and dimension tables"""
        logger.info("Creating dimensional model...")
        
        # 1. Create raw data table
        logger.info("  Creating raw data table...")
        self.conn.register('raw_data', df)
        self.conn.execute("CREATE TABLE IF NOT EXISTS raw_weather AS SELECT * FROM raw_data")
        
        # 2. Create location dimension
        logger.info("  Creating location_dim...")
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS location_dim AS
            SELECT DISTINCT
                ROW_NUMBER() OVER (ORDER BY Tinh_thanh) as location_id,
                Tinh_thanh as province_name,
                COUNT(*) as record_count
            FROM raw_weather
            GROUP BY Tinh_thanh
        """)
        
        # 3. Create date dimension
        logger.info("  Creating date_dim...")
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS date_dim AS
            WITH date_range AS (
                SELECT DISTINCT DATE(Datetime) as date_value
                FROM raw_weather
                ORDER BY date_value
            )
            SELECT
                DATE(date_value) as date_key,
                EXTRACT(YEAR FROM date_value) as year,
                EXTRACT(MONTH FROM date_value) as month,
                EXTRACT(DAY FROM date_value) as day,
                EXTRACT(QUARTER FROM date_value) as quarter,
                EXTRACT(WEEK FROM date_value) as week_number,
                EXTRACT(DAYOFWEEK FROM date_value) as day_of_week,
                CASE WHEN EXTRACT(MONTH FROM date_value) BETWEEN 5 AND 9 THEN 'Mưa'
                     WHEN EXTRACT(MONTH FROM date_value) IN (11, 12, 1, 2) THEN 'Khô'
                     ELSE 'Chuyển tiếp' END as season
            FROM date_range
        """)
        
        # 4. Create main fact table - daily aggregates
        logger.info("  Creating weather_daily_facts...")
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS weather_daily_facts AS
            SELECT
                DATE(rw.Datetime) as date_key,
                rw.Tinh_thanh as province_name,
                ld.location_id,
                
                -- Temperature metrics
                COALESCE(AVG(CAST("Nhiệt độ (°C)" AS FLOAT)), 0) as avg_temp_c,
                COALESCE(MAX(CAST("Nhiệt độ tối đa ngày (°C)" AS FLOAT)), 0) as max_temp_c,
                COALESCE(MIN(CAST("Nhiệt độ tối thiểu ngày (°C)" AS FLOAT)), 0) as min_temp_c,
                
                -- Humidity
                COALESCE(AVG(CAST("Độ ẩm tương đối (%)" AS FLOAT)), 0) as avg_humidity_pct,
                
                -- Precipitation
                COALESCE(SUM(CAST("Lượng mưa (mm)" AS FLOAT)), 0) as total_precipitation_mm,
                
                -- Wind
                COALESCE(AVG(CAST("Tốc độ gió 10m (m/s)" AS FLOAT)), 0) as avg_wind_speed,
                COALESCE(MAX(CAST("Tốc độ gió giật 10m (m/s)" AS FLOAT)), 0) as max_gust_speed,
                
                -- Cloud cover
                COALESCE(AVG(CAST("Độ phủ mây (%)" AS FLOAT)), 0) as avg_cloud_cover_pct,
                
                -- Pressure
                COALESCE(AVG(CAST("Áp suất mực biển (hPa)" AS FLOAT)), 0) as avg_pressure_hpa,
                
                -- Weather code
                COALESCE(MAX(CAST("Mã thời tiết ngày" AS INT)), 0) as weather_code,
                
                COUNT(*) as record_count
            FROM raw_weather rw
            LEFT JOIN location_dim ld ON rw.Tinh_thanh = ld.province_name
            GROUP BY DATE(rw.Datetime), rw.Tinh_thanh, ld.location_id
        """)
        
        logger.info("✓ Tables created successfully")
    
    def create_views(self):
        """Create summary views for analysis"""
        logger.info("Creating summary views...")
        
        # 1. Monthly summary
        logger.info("  Creating weather_monthly_summary...")
        self.conn.execute("""
            CREATE OR REPLACE VIEW weather_monthly_summary AS
            SELECT
                dd.year,
                dd.month,
                wdf.province_name,
                COUNT(*) as num_days,
                ROUND(AVG(wdf.avg_temp_c), 2) as avg_temperature,
                ROUND(MAX(wdf.max_temp_c), 2) as max_temperature,
                ROUND(MIN(wdf.min_temp_c), 2) as min_temperature,
                ROUND(AVG(wdf.avg_humidity_pct), 2) as avg_humidity,
                ROUND(SUM(wdf.total_precipitation_mm), 2) as total_precipitation,
                ROUND(AVG(wdf.avg_wind_speed), 2) as avg_wind_speed,
                ROUND(AVG(wdf.avg_cloud_cover_pct), 2) as avg_cloud_cover
            FROM weather_daily_facts wdf
            LEFT JOIN date_dim dd ON wdf.date_key = dd.date_key
            GROUP BY dd.year, dd.month, wdf.province_name
        """)
        
        # 2. Temperature trends with moving average
        logger.info("  Creating temperature_trends...")
        self.conn.execute("""
            CREATE OR REPLACE VIEW temperature_trends AS
            SELECT
                wdf.date_key,
                dd.year,
                dd.month,
                wdf.province_name,
                wdf.avg_temp_c,
                ROUND(AVG(wdf.avg_temp_c) OVER (
                    PARTITION BY wdf.province_name 
                    ORDER BY wdf.date_key 
                    ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
                ), 2) as temp_30day_ma,
                wdf.max_temp_c,
                wdf.min_temp_c,
                ROUND(AVG(wdf.total_precipitation_mm) OVER (
                    PARTITION BY wdf.province_name 
                    ORDER BY wdf.date_key 
                    ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
                ), 2) as precip_30day_ma
            FROM weather_daily_facts wdf
            LEFT JOIN date_dim dd ON wdf.date_key = dd.date_key
            ORDER BY wdf.province_name, wdf.date_key DESC
        """)
        
        # 3. Rainfall patterns
        logger.info("  Creating rainfall_patterns...")
        self.conn.execute("""
            CREATE OR REPLACE VIEW rainfall_patterns AS
            SELECT
                dd.year,
                dd.month,
                dd.season,
                wdf.province_name,
                COUNT(*) as num_days,
                ROUND(SUM(wdf.total_precipitation_mm), 2) as total_rainfall,
                ROUND(AVG(wdf.total_precipitation_mm), 2) as avg_daily_rainfall,
                COUNT(CASE WHEN wdf.total_precipitation_mm > 0 THEN 1 END) as rainy_days,
                ROUND(100.0 * COUNT(CASE WHEN wdf.total_precipitation_mm > 0 THEN 1 END) / COUNT(*), 2) as rainy_days_pct
            FROM weather_daily_facts wdf
            LEFT JOIN date_dim dd ON wdf.date_key = dd.date_key
            GROUP BY dd.year, dd.month, dd.season, wdf.province_name
        """)
        
        logger.info("✓ Views created successfully")
    
    def create_indices(self):
        """Create indices for query performance"""
        logger.info("Creating indices for performance...")
        
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_wdf_date ON weather_daily_facts(date_key)")
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_wdf_province ON weather_daily_facts(province_name)")
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_wdf_location ON weather_daily_facts(location_id)")
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_dd_year_month ON date_dim(year, month)")
        
        logger.info("✓ Indices created")
    
    def get_statistics(self) -> dict:
        """Get data statistics"""
        stats = {}
        
        try:
            # Row counts
            stats['raw_weather_rows'] = self.conn.execute(
                "SELECT COUNT(*) FROM raw_weather"
            ).fetchone()[0]
            
            stats['daily_facts_rows'] = self.conn.execute(
                "SELECT COUNT(*) FROM weather_daily_facts"
            ).fetchone()[0]
            
            stats['provinces'] = self.conn.execute(
                "SELECT COUNT(*) FROM location_dim"
            ).fetchone()[0]
            
            stats['date_range'] = self.conn.execute(
                "SELECT MIN(date_key), MAX(date_key) FROM date_dim"
            ).fetchone()
            
            # Data size
            db_size = self.duckdb_path.stat().st_size / (1024**3)
            stats['db_size_gb'] = round(db_size, 2)
            
        except Exception as e:
            logger.warning(f"Could not get statistics: {e}")
        
        return stats


def main():
    """Main entry point"""
    logger.info("="*60)
    logger.info("DUCKDB DATA MART - Build analytics database")
    logger.info("="*60)
    
    mart = DuckDBDataMart(DUCKDB_FILE)
    
    try:
        # Connect to DuckDB
        mart.connect()
        
        # Load data (try Supabase first, fallback to CSV)
        df = mart.load_data()
        
        if df is None or df.empty:
            logger.error("✗ Failed to load data from any source")
            return 1
        
        # Clean data
        df = mart.clean_data(df)
        
        # Create dimensional model
        mart.create_tables(df)
        
        # Create summary views
        mart.create_views()
        
        # Create indices
        mart.create_indices()
        
        # Get statistics
        stats = mart.get_statistics()
        
        # Log summary
        logger.info("="*60)
        logger.info("✅ Data mart built successfully!")
        logger.info(f"   Raw weather records: {stats.get('raw_weather_rows', 'N/A'):,}")
        logger.info(f"   Daily facts: {stats.get('daily_facts_rows', 'N/A'):,}")
        logger.info(f"   Provinces: {stats.get('provinces', 'N/A')}")
        logger.info(f"   Date range: {stats.get('date_range', 'N/A')}")
        logger.info(f"   Database size: {stats.get('db_size_gb', 'N/A')} GB")
        logger.info(f"   Location: {DUCKDB_FILE}")
        logger.info("   Next step: Run powerbi_export.py to export for Power BI")
        logger.info("="*60)
        
        return 0
        
    except Exception as e:
        logger.error(f"✗ Error: {e}", exc_info=True)
        return 1
        
    finally:
        mart.close()


if __name__ == '__main__':
    exit_code = main()
    sys.exit(exit_code)
