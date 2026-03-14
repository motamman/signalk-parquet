# <img src="public/parquet.png" alt="SignalK Parquet Data Store" width="72" height="72" style="vertical-align: middle; margin-right: 20px;"> SignalK Parquet Data Store

A comprehensive SignalK plugin and webapp that saves SignalK data directly to Parquet files with manual and automated regimen-based archiving and advanced querying features, including a REST API built on the SignalK History API and spatial geographic analysis capabilities. 

## Features

### Core Data Management
- **Smart Data Types**: Intelligent Parquet schema detection preserves native data types (DOUBLE, BOOLEAN) instead of forcing everything to strings
- **Multiple File Formats**: Support for Parquet, JSON, and CSV output formats (querying in parquet only)
- **Daily Export Pipeline**: Simplified daily export creates consolidated Parquet files directly
  - Data accumulates in SQLite buffer throughout the day
  - Single export at configurable hour (default: 4 AM UTC)
  - No separate consolidation step needed
- **SQLite WAL Buffering**: Crash-safe data ingestion with Write-Ahead Logging
  - Replaces in-memory buffers with persistent SQLite database
  - Automatic recovery after power loss or crashes
  - 48-hour retention for federated queries
  - Per-path tables (`buffer_navigation_position`, etc.) for partition-aligned access
  - `buffer_tables` metadata table tracks path→table mapping
- **🆕 Hive-Partitioned Storage**: Efficient file organization for query performance
  - Structure: `tier=raw/context={ctx}/path={path}/year={year}/day={day}/`
  - Aggregation tiers: `raw`, `5s`, `60s`, `1h`
  - Automatic partition pruning for time-range queries
- **🆕 Cloud Federated Querying**: Query historical data directly from S3 or Cloudflare R2 using DuckDB's native support
  - Three-tier query hierarchy: local parquet → cloud supplement → SQLite buffer
  - Automatic partition pruning reduces data transfer by 70-90%
  - Predicate pushdown filters data at source before transfer
- **🆕 Auto-Discovery**: Automatically configure paths when first queried
  - On-demand path configuration when History API queries unconfigured paths
  - Include/exclude glob patterns for fine-grained control
  - Optional live data requirement before configuration
- **🆕 Vector Averaging**: Correct aggregation of angular data (headings, bearings, wind angles)
  - Automatic detection of angular paths via SignalK metadata (`units === 'rad'`)
  - Uses `atan2(mean(sin), mean(cos))` instead of arithmetic mean
  - Lossless re-aggregation across tiers via stored sin/cos averages
- **🆕 Buffer Bucketing**: SQLite buffer data bucketed to match Parquet query resolution
  - Prevents raw per-second records from flooding bucketed query results
  - Supports all aggregate methods including vector averaging for angular paths

### Data Validation & Schema Repair
- **NEW Schema Validation**: Comprehensive validation of Parquet file schemas against SignalK metadata standards
- **NEW Automated Repair**: One-click repair of schema violations with proper data type conversion
- **NEW Type Correction**: Automatic conversion of incorrectly stored data types (e.g., numeric strings → DOUBLE, boolean strings → BOOLEAN)
- **NEW Metadata Integration**: Uses SignalK metadata (units, types) to determine correct data types for marine measurements
- **NEW Safe Operations**: Creates backups before repair and quarantines corrupted files for safety
- *NEW *Progress Tracking**: Real-time progress monitoring with cancellation support for large datasets

#### Benefits of Proper Data Types
Using correct data types in Parquet files provides significant advantages:
- **Storage Efficiency**: Numeric data stored as DOUBLE uses ~50% less space than string representations
- **Query Performance**: Native numeric operations are 5-10x faster than string parsing during analysis
- **Data Integrity**: Type validation prevents data corruption and ensures consistent analysis results
- **Analytics Compatibility**: Proper types enable advanced statistical analysis and machine learning applications
- **Compression**: Parquet's columnar compression works optimally with correctly typed data

#### Validation Process
The validation system checks each Parquet file for:
- **Field Type Consistency**: Ensures numeric marine data (position, speed, depth) is stored as DOUBLE
- **Boolean Representation**: Validates true/false values are stored as BOOLEAN, not strings
- **Metadata Alignment**: Compares file schemas against SignalK metadata for units like meters, volts, amperes
- **Schema Standards**: Enforces data best practices for long-term data integrity

### Advanced Querying
- **SignalK History API Compliance**: Full compliance with SignalK History API specifications
  - **Standard Time Parameters**: All 5 standard query patterns supported
  - **Time-Filtered Discovery**: Paths and contexts filtered by time range
  - **Optional Analytics**: Moving averages (EMA/SMA) available on demand
- **🌍 ISO 8601 Timestamps**: All timestamps returned in server local time with offset (e.g., `2025-10-20T12:34:04-04:00`)
- **Flexible Time Querying**: Multiple ways to specify time ranges
  - Query from now, from specific times, or between time ranges
  - Duration-based windows (1h, 30m, 2d) for easy relative queries
  - Forward and backward time querying support
- **Time Alignment**: Automatic alignment of data from different sensors using time bucketing
- **DuckDB Integration**: Direct SQL querying of Parquet files with type-safe operations
- **🌍 Spatial Analysis**: Advanced geographic analysis with DuckDB spatial extension
  - **Track Analysis**: Calculate vessel tracks, distances, and movement patterns
  - **Proximity Detection**: Multi-vessel distance calculations and collision risk analysis
  - **Geographic Visualization**: Generate movement boundaries, centroids, and spatial statistics
  - **Route Planning**: Historical track analysis for route optimization and performance analysis
  - **🆕 Spatial Correlation**: Filter any sensor data by vessel location
    - Query "wind data when vessel was within this area"
    - Bounding box (`bbox`) and radius filters work on all paths
    - Automatically correlates timestamps with position data

### Management & Control
- **Command Management**: Register, execute, and manage SignalK commands with automatic path configuration
- **Regimen-Based Data Collection**: Control data collection with command-based regimens
- **Multi-Vessel Support**: Wildcard vessel contexts (`vessels.*`) with MMSI-based exclusion filtering
- **Source Filtering**: Filter data by SignalK source labels (bypasses server arbitration for raw data access)
- **Comprehensive REST API**: Full programmatic control of queries and configuration

### User Interface & Integration
- **Responsive Web Interface**: Complete web-based management interface
- **Cloud Storage Integration**: Upload files to Amazon S3 or Cloudflare R2 as part of the daily export pipeline
- **Context Support**: Support for multiple vessel contexts with exclusion controls

### Regimen System (Advanced)
- **Operational Context Tracking**: Define regimens for operational states (mooring, anchoring, racing, passage-making)
- **Command-Based Episodes**: Track state transitions using SignalK commands as regimen triggers
- **Episode Boundary Detection**: Sophisticated SQL-based detection of operational periods using CTEs and window functions
- **Contextual Data Collection**: Link SignalK paths to regimens for targeted data analysis during specific operations
- **Web Interface Management**: Create, edit, and manage regimens and command keywords through the web UI

### NEW Threshold Automation
- **NEW Per-Command Conditions**: Each regimen/command can define one or more thresholds that watch a single SignalK path.
- **NEW True-Only Actions**: On every path update the condition is evaluated; when it is true the command is set to the threshold's `activateOnMatch` state (ON/OFF). False evaluations leave the command untouched, so use a second threshold if you want a different level to switch it back.
- **NEW Stable Triggers**: Optional hysteresis (seconds) suppresses re-firing while the condition remains true, preventing rapid toggling in noisy data.
- **NEW Multiple Thresholds Per Path**: Unique monitor keys allow several thresholds to observe the same SignalK path without cancelling each other.
- **NEW Unit Handling**: Threshold values must match the live SignalK units (e.g., fractional 0–1 SoC values). Angular thresholds are entered in degrees in the UI and stored as radians automatically.
- **NEW Automation State Machine**: When enabling automation, command is set to OFF then all thresholds are immediately evaluated. When disabling automation, threshold monitoring stops and command state remains unchanged. Default state is hardcoded to OFF on server side.

- **Custom Analysis**: Create custom analysis prompts for specific operational needs

## Requirements

### Core Requirements
- SignalK Server v2.13+
- Node.js 22.5+ (required for `node:sqlite` — the built-in SQLite module used for crash-safe buffering; on Node < 22.5 the buffer falls back to in-memory LRU)

## Installation

### Install from GitHub
```bash
# Navigate to folder
cd ~/.signalk/node_modules/

# Install from npm (after publishing)
npm install signalk-parquet

# Or install from GitHub
npm install motamman/signalk-parquet
cd ~/.signalk/node_modules/signalk-parquet
npm run build

# Restart SignalK
sudo systemctl restart signalk
```

### Development Setup

```bash
# Clone or copy the signalk-parquet directory
cd signalk-parquet

# Install dependencies
npm install

# Build the TypeScript code
npm run build

# Copy to SignalK plugins directory
cp -r . ~/.signalk/node_modules/signalk-parquet/

# Restart SignalK
sudo systemctl restart signalk
```

### Production Build

```bash
# Build for production
npm run build

# The compiled JavaScript will be in the dist/ directory
```

## Configuration

### Plugin Configuration

Navigate to **SignalK Admin → Server → Plugin Config → SignalK Parquet Data Store**

Configure basic plugin settings (path configuration is managed separately in the web interface):

| Setting | Description | Default |
|---------|-------------|---------|
| **Buffer Size** | Number of records to buffer before writing | 1000 |
| **Save Interval** | How often to save buffered data (seconds) | 30 |
| **Output Directory** | Directory to save data files | SignalK data directory |
| **Filename Prefix** | Prefix for generated filenames | `signalk_data` |
| **File Format** | Output format (parquet, json, csv) | `parquet` |
| **Retention Days** | Days to keep processed files | 7 |
| **Daily Export Hour** | Hour (0-23 UTC) to run daily Parquet export | 4 |
| **Export Batch Size** | Max records to export per cycle (1,000-200,000) | 50000 |
| **Buffer Retention Hours** | How long to keep exported records in SQLite (hours) | 48 |
| **Enable Raw SQL** | Enable /api/query endpoint for raw SQL queries | `false` |

### Auto-Discovery Configuration

Configure automatic path discovery when querying unconfigured paths:

| Setting | Description | Default |
|---------|-------------|---------|
| **Enable Auto-Discovery** | Master switch for auto-discovery | `false` |
| **Require Live Data** | Only configure if path has live SignalK data | `true` |
| **Max Auto-Configured Paths** | Maximum number of auto-configured paths | `100` |
| **Include Patterns** | Glob patterns for paths to include (e.g., `navigation.*`) | `[]` |
| **Exclude Patterns** | Glob patterns for paths to exclude (e.g., `propulsion.*`) | `[]` |

When enabled, Auto-Discovery will automatically add path configurations when:
1. A History API query requests data for an unconfigured path
2. The path matches include patterns (if specified)
3. The path doesn't match exclude patterns
4. The path has live data in SignalK (if `requireLiveData` is enabled)

Auto-discovered paths are marked with the `autoDiscovered: true` flag and have auto-generated human-readable names prefixed with `[Auto]`.

### Cloud Upload Configuration

Configure cloud storage upload in the plugin configuration. Uploads run as part of the daily export pipeline.

| Setting | Description | Default |
|---------|-------------|---------|
| **Provider** | Cloud provider: `none`, `s3`, or `r2` | `none` |
| **Bucket** | Bucket name | - |
| **Region** | AWS region (S3 only) | `us-east-1` |
| **Account ID** | Cloudflare account ID (R2 only) | - |
| **Key Prefix** | Object key prefix | - |
| **Access Key ID** | Cloud credentials | - |
| **Secret Access Key** | Cloud credentials | - |
| **Delete After Upload** | Delete local files after upload | `false` |

## Path Configuration

**Important**: Path configuration is managed exclusively through the web interface, not in the SignalK admin interface. This provides a more intuitive interface for managing data collection paths.

### Accessing Path Configuration

1. Navigate to: `http://localhost:3000/plugins/signalk-parquet`
2. Click the **⚙️ Path Configuration** tab

### Adding Data Paths

Use the web interface to configure which SignalK paths to collect:

1. Click **➕ Add New Path**
2. Configure the path settings:
   - **SignalK Path**: The SignalK data path (e.g., `navigation.position`)
   - **Always Enabled**: Collect data regardless of regimen state
   - **Regimen Control**: Command name that controls collection
   - **Source Filter**: Only collect from specific sources
   - **Context**: SignalK context (`vessels.self`, `vessels.*`, or specific vessel)
   - **Exclude MMSI**: For `vessels.*` context, exclude specific MMSI numbers
3. Click **✅ Add Path**

### Managing Existing Paths

- **Edit Path**: Click ✏️ Edit button to modify path settings
- **Delete Path**: Click 🗑️ Remove button to delete a path
- **Refresh**: Click 🔄 Refresh Paths to reload configuration
- **Show/Hide Commands**: Toggle button to show/hide command paths in the table

### Command Management

The plugin streamlines command management with automatic path configuration:

1. **Register Command**: Commands are automatically registered with enabled path configurations
2. **Start Command**: Click **Start** button to activate a command regimen
3. **Stop Command**: Click **Stop** button to deactivate a command regimen
4. **Remove Command**: Click **Remove** button to delete a command and its path configuration

This eliminates the previous 3-step process of registering commands, adding paths, and enabling them separately.

### Path Configuration Storage

Path configurations are stored separately from plugin configuration in:
```
~/.signalk/signalk-parquet/webapp-config.json
```

This allows for:
- Independent management of path configurations
- Better separation of concerns
- Easier backup and migration of path settings
- More intuitive web-based configuration interface

### Regimen-Based Control

Regimens allow you to control data collection based on SignalK commands:

**Example**: Weather data collection with source filtering
```json
{
  "path": "environment.wind.angleApparent",
  "enabled": false,
  "regimen": "captureWeather",
  "source": "mqtt-weatherflow-udp",
  "context": "vessels.self"
}
```

**Note**: Source filtering accesses raw data before SignalK server arbitration, allowing collection of data from specific sources that might otherwise be filtered out.

**Multi-Vessel Example**: Collect navigation data from all vessels except specific MMSI numbers
```json
{
  "path": "navigation.position",
  "enabled": true,
  "context": "vessels.*",
  "excludeMMSI": ["123456789", "987654321"]
}
```

**Command Path**: Command paths are automatically created when registering commands
```json
{
  "path": "commands.captureWeather",
  "enabled": true,
  "context": "vessels.self"
}
```

This path will only collect data when the command `commands.captureWeather` is active.

## TypeScript Architecture

### Type Safety

The plugin uses comprehensive TypeScript interfaces:

```typescript
interface PluginConfig {
  bufferSize: number;
  saveIntervalSeconds: number;
  outputDirectory: string;
  filenamePrefix: string;
  fileFormat: 'json' | 'csv' | 'parquet';
  retentionDays: number;
  vesselMMSI: string;
  cloudUpload: CloudUploadConfig;
  enableStreaming?: boolean;
  useSqliteBuffer?: boolean;
  exportBatchSize?: number;
  bufferRetentionHours?: number;
  useHivePartitioning?: boolean;
  dailyExportHour?: number;
  autoDiscovery?: AutoDiscoveryConfig;
  enableRawSql?: boolean;
}

interface CloudUploadConfig {
  provider: 'none' | 's3' | 'r2';
  bucket?: string;
  region?: string;       // S3 only
  accountId?: string;    // R2 only (Cloudflare account ID)
  keyPrefix?: string;
  accessKeyId?: string;
  secretAccessKey?: string;
  deleteAfterUpload?: boolean;
}

interface PathConfig {
  path: string;
  name?: string;
  enabled?: boolean;
  regimen?: string;
  source?: string;
  context?: string;
  excludeMMSI?: string[];
  autoDiscovered?: boolean;
}

interface DataRecord {
  received_timestamp: string;
  signalk_timestamp: string;
  context: string;
  path: string;
  value: any;
  source_label?: string;
  meta?: string;
}
```

### Plugin State Management

The plugin maintains typed state:

```typescript
interface PluginState {
  unsubscribes: Array<() => void>;
  dataBuffers: LRUCache<string, DataRecord[]>;
  activeRegimens: Set<string>;
  subscribedPaths: Set<string>;
  parquetWriter?: ParquetWriter;
  cloudClient?: any;
  currentConfig?: PluginConfig;
  sqliteBuffer?: SQLiteBufferInterface;
  exportService?: ParquetExportServiceInterface;
  commandState: CommandRegistrationState;
}
```

### Express Router Types

API routes are fully typed:

```typescript
router.get('/api/paths', 
  (_: TypedRequest, res: TypedResponse<PathsApiResponse>) => {
    // Typed request/response handling
  }
);
```

## Data Output Structure

### File Organization (Hive-Partitioned)

The plugin uses Hive-style partitioned paths for efficient querying:

```
output_directory/
├── tier=raw/
│   ├── context=vessels__self/
│   │   ├── path=navigation__position/
│   │   │   ├── year=2025/
│   │   │   │   ├── day=197/
│   │   │   │   │   ├── data_20250716T120000.parquet
│   │   │   │   │   └── data_20250716T130000.parquet
│   │   │   │   └── day=198/
│   │   │   │       └── data_20250717T080000.parquet
│   │   │   └── year=2024/
│   │   │       └── day=365/
│   │   └── path=navigation__speedOverGround/
│   └── context=vessels__urn-mrn-imo-mmsi-368396230/
│       └── path=navigation__position/
├── tier=5s/
│   └── [aggregated 5-second data]
├── tier=60s/
│   └── [aggregated 1-minute data]
├── tier=1h/
│   └── [aggregated hourly data]
├── buffer.db              <- SQLite WAL buffer (48h retention)
└── buffer.db-wal          <- Write-ahead log
```

**Partition Structure:**
- `tier=` - Aggregation level: `raw`, `5s`, `60s`, `1h`
- `context=` - Vessel context (sanitized: `.` → `__`, `:` → `-`)
- `path=` - SignalK path (sanitized: `.` → `__`)
- `year=` - Year (e.g., `2025`)
- `day=` - Day of year, zero-padded (e.g., `197`)

> **Legacy flat structure**: If you have data from pre-Hive versions, use the Migration API to convert to Hive partitioning. See [Data Migration](#data-migration).

## Data Migration

### Migrating Legacy Files to Hive Partitioning

If you have existing data in the legacy flat structure, use the Migration API to convert to Hive partitioning:

**1. Scan for migratable files:**
```bash
curl -X POST http://localhost:3000/plugins/signalk-parquet/api/migrate/scan \
  -H "Content-Type: application/json" \
  -d '{"sourceDirectory": "/path/to/data"}'
```

Response includes:
- Total files to migrate
- Total size in bytes
- Files grouped by SignalK path
- Estimated migration time

**2. Start migration:**
```bash
curl -X POST http://localhost:3000/plugins/signalk-parquet/api/migrate \
  -H "Content-Type: application/json" \
  -d '{
    "sourceDirectory": "/path/to/data",
    "targetDirectory": "/path/to/data",
    "targetTier": "raw",
    "deleteSourceAfterMigration": false
  }'
```

**3. Check progress:**
```bash
curl http://localhost:3000/plugins/signalk-parquet/api/migrate/progress/{jobId}
```

**4. Cancel if needed:**
```bash
curl -X POST http://localhost:3000/plugins/signalk-parquet/api/migrate/cancel/{jobId}
```

**Migration Options:**
| Option | Description | Default |
|--------|-------------|---------|
| `sourceDirectory` | Source directory to scan | Plugin data directory |
| `targetDirectory` | Target directory for Hive files | Same as source |
| `targetTier` | Target aggregation tier | `raw` |
| `deleteSourceAfterMigration` | Delete source files after successful migration | `false` |

### Data Schema

Each record contains:

| Field | Type | Description |
|-------|------|-------------|
| `received_timestamp` | string | When the plugin received the data |
| `signalk_timestamp` | string | Original SignalK timestamp |
| `context` | string | SignalK context (e.g., `vessels.self`) |
| `path` | string | SignalK path |
| `value` | DOUBLE/BOOLEAN/INT64/UTF8 | **Smart typed values** - numbers stored as DOUBLE, booleans as BOOLEAN, etc. |
| `value_json` | string | JSON representation for complex values |
| `source` | string | Complete source information |
| `source_label` | string | Source label |
| `source_type` | string | Source type |
| `source_pgn` | number | PGN number (if applicable) |
| `meta` | string | Metadata information |

#### Aggregated Tier Schema (5s, 60s, 1h)

Aggregated tiers use a different schema optimized for statistical queries:

| Field | Type | Description |
|-------|------|-------------|
| `bucket_time` | TIMESTAMP | Start of the aggregation time bucket |
| `context` | UTF8 | SignalK context |
| `path` | UTF8 | SignalK path |
| `value_avg` | DOUBLE | Average value in bucket |
| `value_min` | DOUBLE | Minimum value (NULL for angular paths) |
| `value_max` | DOUBLE | Maximum value (NULL for angular paths) |
| `sample_count` | INT64 | Number of raw samples aggregated |
| `value_sin_avg` | DOUBLE | Average of sin(value) — angular paths only, for lossless re-aggregation |
| `value_cos_avg` | DOUBLE | Average of cos(value) — angular paths only, for lossless re-aggregation |

#### Smart Data Types

The plugin now intelligently detects and preserves native data types:

- **Numbers**: Stored as `DOUBLE` (floating point) or `INT64` (integers)
- **Booleans**: Stored as `BOOLEAN` 
- **Strings**: Stored as `UTF8`
- **Objects**: Serialized to JSON and stored as `UTF8`
- **Mixed Types**: Falls back to `UTF8` when a path contains multiple data types

This provides better compression, faster queries, and proper type safety for data analysis.

## Web Interface

### Features

- **Path Configuration**: Manage data collection paths with multi-vessel support
- **Command Management**: Streamlined command registration and control
- **Data Exploration**: Browse available data paths
- **SQL Queries**: Execute DuckDB queries against Parquet files
- **History API**: Query historical data using SignalK History API endpoints
- **Cloud Status**: Test cloud storage connectivity and configuration
- **Responsive Design**: Works on desktop and mobile
- **MMSI Filtering**: Exclude specific vessels from wildcard contexts

### API Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/paths` | GET | List available data paths |
| `/api/files/:path` | GET | List files for a path |
| `/api/sample/:path` | GET | Sample data from a path |
| `/api/query` | POST | Execute SQL query (⚠️ disabled by default, requires `SIGNALK_PARQUET_RAW_SQL=true`) |
| `/api/config/paths` | GET/POST/PUT/DELETE | Manage path configurations |
| `/api/test-cloud` | POST | Test cloud storage connection |
| `/api/health` | GET | Health check |
| `/api/version` | GET | Plugin version |
| `/api/store/stats` | GET | Data store statistics |
| **SignalK History API** | | |
| `/signalk/v1/history/values` | GET | SignalK History API - Get historical values |
| `/signalk/v1/history/contexts` | GET | SignalK History API - Get available contexts |
| `/signalk/v1/history/paths` | GET | SignalK History API - Get available paths |
| `/signalk/v2/api/history/*` | GET | SignalK v2 API - handled by registered HistoryApi provider (spec-compliant) |
| **Migration API** | | |
| `/api/migrate/scan` | POST | Scan directory for migratable files |
| `/api/migrate` | POST | Start migration job |
| `/api/migrate/progress/:jobId` | GET | Get migration job progress |
| `/api/migrate/cancel/:jobId` | POST | Cancel running migration job |
| `/api/migrate/jobs` | GET | List all migration jobs |
| **Buffer Status API** | | |
| `/api/buffer/stats` | GET | Get SQLite buffer statistics |
| `/api/buffer/export` | POST | Force immediate export of pending records |
| `/api/buffer/health` | GET | Get buffer health status |
| **Validation & Repair API** | | |
| `/api/validate-schemas` | POST | Scan and validate Parquet schemas |
| `/api/validate-schemas/progress/:jobId` | GET | Get validation progress |
| `/api/validate-schemas/cancel/:jobId` | POST | Cancel validation job |
| `/api/repair-schemas` | POST | Repair schema violations |
| `/api/repair-schemas/progress/:jobId` | GET | Get repair progress |
| `/api/repair-schemas/cancel/:jobId` | POST | Cancel repair job |
| **Vector Averaging Migration** | | |
| `/api/migrate/vector-averaging` | POST | Migrate to vector averaging aggregation |
| `/api/migrate/vector-averaging/:jobId` | GET | Get migration progress |
| `/api/migrate/vector-averaging/cancel/:jobId` | POST | Cancel migration |
| **Aggregation API** | | |
| `/api/aggregate` | POST | Trigger aggregation for all tiers |
| `/api/aggregate/:sourceTier/:targetTier` | POST | Aggregate specific tier pair |
| **Cloud Sync API** | | |
| `/api/cloud/compare` | POST | Compare local vs cloud data |
| `/api/cloud/compare/:jobId` | GET | Get comparison progress |
| `/api/cloud/sync` | POST | Sync data to cloud |
| `/api/cloud/sync/:jobId` | GET | Get sync progress |

## DuckDB Integration

### Query Examples

#### Basic Queries
```sql
-- Get latest 10 records from navigation position
SELECT * FROM read_parquet('/path/to/navigation/position/*.parquet', union_by_name=true)
ORDER BY received_timestamp DESC LIMIT 10;

-- Count total records
SELECT COUNT(*) FROM read_parquet('/path/to/navigation/position/*.parquet', union_by_name=true);

-- Filter by source
SELECT * FROM read_parquet('/path/to/environment/wind/*.parquet', union_by_name=true)
WHERE source_label = 'mqtt-weatherflow-udp'
ORDER BY received_timestamp DESC LIMIT 100;

-- Aggregate by hour
SELECT
  DATE_TRUNC('hour', received_timestamp::timestamp) as hour,
  AVG(value::double) as avg_value,
  COUNT(*) as record_count
FROM read_parquet('/path/to/data/*.parquet', union_by_name=true)
GROUP BY hour
ORDER BY hour;
```

#### 🌍 Spatial Analysis Queries

```sql
-- Calculate distance traveled over time
WITH ordered_positions AS (
  SELECT
    signalk_timestamp,
    ST_Point(value_longitude, value_latitude) as position,
    LAG(ST_Point(value_longitude, value_latitude)) OVER (ORDER BY signalk_timestamp) as prev_position
  FROM read_parquet('data/vessels/urn_mrn_imo_mmsi_368396230/navigation/position/*.parquet', union_by_name=true)
  WHERE signalk_timestamp >= '2025-09-27T16:00:00Z'
    AND signalk_timestamp <= '2025-09-27T23:59:59Z'
    AND value_latitude IS NOT NULL AND value_longitude IS NOT NULL
),
distances AS (
  SELECT *,
    CASE
      WHEN prev_position IS NOT NULL
      THEN ST_Distance_Sphere(position, prev_position)
      ELSE 0
    END as distance_meters
  FROM ordered_positions
)
SELECT
  strftime(date_trunc('hour', signalk_timestamp::TIMESTAMP), '%Y-%m-%dT%H:%M:%SZ') as time_bucket,
  AVG(value_latitude) as avg_lat,
  AVG(value_longitude) as avg_lon,
  ST_AsText(ST_Centroid(ST_Collect(position))) as centroid,
  SUM(distance_meters) as total_distance_meters,
  COUNT(*) as position_records,
  ST_AsText(ST_ConvexHull(ST_Collect(position))) as movement_area
FROM distances
GROUP BY time_bucket
ORDER BY time_bucket;

-- Multi-vessel proximity analysis
SELECT
  v1.context as vessel1,
  v2.context as vessel2,
  ST_Distance_Sphere(
    ST_Point(v1.value_longitude, v1.value_latitude),
    ST_Point(v2.value_longitude, v2.value_latitude)
  ) as distance_meters,
  v1.signalk_timestamp
FROM read_parquet('data/vessels/*/navigation/position/*.parquet', union_by_name=true) v1
JOIN read_parquet('data/vessels/*/navigation/position/*.parquet', union_by_name=true) v2
  ON v1.signalk_timestamp = v2.signalk_timestamp AND v1.context != v2.context
WHERE v1.signalk_timestamp >= '2025-09-27T00:00:00Z'
  AND ST_Distance_Sphere(
    ST_Point(v1.value_longitude, v1.value_latitude),
    ST_Point(v2.value_longitude, v2.value_latitude)
  ) < 1000  -- Within 1km
ORDER BY distance_meters;

-- Advanced movement analysis with bounding boxes
WITH ordered_positions AS (
  SELECT
    signalk_timestamp,
    ST_Point(value_longitude, value_latitude) as position,
    value_latitude,
    value_longitude,
    LAG(ST_Point(value_longitude, value_latitude)) OVER (ORDER BY signalk_timestamp) as prev_position,
    strftime(date_trunc('hour', signalk_timestamp::TIMESTAMP), '%Y-%m-%dT%H:%M:%SZ') as time_bucket
  FROM read_parquet('data/vessels/urn_mrn_imo_mmsi_368396230/navigation/position/*.parquet', union_by_name=true)
  WHERE signalk_timestamp >= '2025-09-27T16:00:00Z'
    AND signalk_timestamp <= '2025-09-27T23:59:59Z'
    AND value_latitude IS NOT NULL AND value_longitude IS NOT NULL
),
distances AS (
  SELECT *,
    CASE
      WHEN prev_position IS NOT NULL
      THEN ST_Distance_Sphere(position, prev_position)
      ELSE 0
    END as distance_meters
  FROM ordered_positions
)
SELECT
  time_bucket,
  AVG(value_latitude) as avg_lat,
  AVG(value_longitude) as avg_lon,
  -- Calculate bounding box manually
  MIN(value_latitude) as min_lat,
  MAX(value_latitude) as max_lat,
  MIN(value_longitude) as min_lon,
  MAX(value_longitude) as max_lon,
  -- Distance and movement metrics
  SUM(distance_meters) as total_distance_meters,
  ROUND(SUM(distance_meters) / 1000.0, 2) as total_distance_km,
  COUNT(*) as position_records,
  -- Movement area approximation using bounding box
  (MAX(value_latitude) - MIN(value_latitude)) * 111320 *
  (MAX(value_longitude) - MIN(value_longitude)) * 111320 *
  COS(RADIANS(AVG(value_latitude))) as approx_area_m2
FROM distances
GROUP BY time_bucket
ORDER BY time_bucket;
```

#### Available Spatial Functions
- `ST_Point(longitude, latitude)` - Create point geometries
- `ST_Distance_Sphere(point1, point2)` - Calculate distances in meters
- `ST_AsText(geometry)` - Convert to Well-Known Text format
- `ST_Centroid(ST_Collect(points))` - Find center of multiple points
- `ST_ConvexHull(ST_Collect(points))` - Create movement boundary polygons

## History API Integration

The plugin provides full SignalK History API compliance, allowing you to query historical data using standard SignalK API endpoints with enhanced performance and filtering capabilities.

### Available Endpoints

| Endpoint | Description | Parameters |
|----------|-------------|------------|
| `/signalk/v1/history/values` | Get historical values for specified paths | **Standard patterns** (see below)<br>**Optional**: `resolution`, `includeMovingAverages`, `bbox`, `radius` |
| `/signalk/v1/history/contexts` | Get available vessel contexts for time range | **Time Range**: Any standard pattern (see below) ⚠️<br>Returns only contexts with data in specified range |
| `/signalk/v1/history/paths` | Get available SignalK paths for time range | **Time Range**: Any standard pattern (see below) ⚠️<br>Returns only paths with data in specified range |
| `/signalk/v2/api/history/*` | **Spec-compliant** - handled by registered `HistoryApi` provider | Per SignalK spec (ISO 8601 durations, no extensions) |

> **Note:** V2 routes (`/signalk/v2/api/history/*`) are handled by the registered `HistoryApi` provider (`history-provider.ts`) for SignalK server multi-provider support. V1 routes include signalk-parquet extensions (spatial filtering, shorthand durations, etc.) not available in V2.

> ⚠️ **Extension**: The `/contexts` and `/paths` endpoints accept time range parameters as **optional**. The official spec requires time parameters; without them, these endpoints return all available data (more permissive behavior).

### Standard Time Range Patterns

The History API supports 5 standard SignalK time query patterns:

| Pattern | Parameters | Description | Example |
|---------|-----------|-------------|---------|
| **1** | `duration` | Query back from now | `?duration=1h` |
| **2** | `from` + `duration` | Query forward from start | `?from=2025-01-01T00:00:00Z&duration=1h` |
| **3** | `to` + `duration` | Query backward to end | `?to=2025-01-01T12:00:00Z&duration=1h` |
| **4** | `from` | From start to now | `?from=2025-01-01T00:00:00Z` |
| **5** | `from` + `to` | Specific range | `?from=2025-01-01T00:00:00Z&to=2025-01-02T00:00:00Z` |

### Query Parameters

| Parameter | Description | Format | Examples |
|-----------|-------------|---------|----------|
| **Required for `/values`:** | | | |
| `paths` | SignalK paths with optional aggregation | `path:method` | `navigation.speedOverGround:average` |
| **Time Range:** | Use one of the 5 standard patterns above | | |
| `duration` | Time period (see Duration Formats below) | Multiple formats | `PT1H`, `3600`, `1h` |
| `from` | Start time (ISO 8601) | ISO datetime | `2025-01-01T00:00:00Z` |
| `to` | End time (ISO 8601) | ISO datetime | `2025-01-01T06:00:00Z` |
| **Optional:** | | | |
| `context` | Vessel context | `vessels.self` or `vessels.<id>` | `vessels.self` (default) |
| `resolution` | Time bucket size in **seconds** | Seconds or time expression | `60`, `1m` (1 minute buckets) |

#### Duration Formats

| Format | Example | Description |
|--------|---------|-------------|
| ISO 8601 | `PT1H`, `PT30M`, `P1D`, `PT1H30M` | Standard ISO duration |
| Integer seconds | `3600`, `60` | Plain number as seconds |
| Shorthand ⚠️ | `1h`, `30m`, `5s`, `2d` | Human-friendly format (extension) |

> ⚠️ Shorthand format is a non-standard extension for convenience. Use ISO 8601 or integer seconds for maximum compatibility.

#### Resolution Parameter

> **BREAKING CHANGE (v0.7.0+)**: Resolution is now in **seconds** (was milliseconds).

| Old (v0.6.x) | New (v0.7.0+) |
|--------------|---------------|
| `?resolution=60000` | `?resolution=60` or `?resolution=1m` |
| `?resolution=5000` | `?resolution=5` or `?resolution=5s` |
| `?resolution=300000` | `?resolution=300` or `?resolution=5m` |

#### Aggregation Methods

| Method | Description | Example |
|--------|-------------|---------|
| `average` | Average of values in bucket | `path:average` |
| `min` | Minimum value in bucket | `path:min` |
| `max` | Maximum value in bucket | `path:max` |
| `first` | First value in bucket | `path:first` |
| `last` | Last value in bucket | `path:last` |
| `mid` | Median value in bucket | `path:mid` |
| `sma` | Simple Moving Average (returns only smoothed value) | `path:sma:5` |
| `ema` | Exponential Moving Average (returns only smoothed value) | `path:ema:0.2` |

**SMA/EMA as aggregation methods (official SignalK syntax):**
```bash
# SMA with window of 5 - returns ONLY the smoothed value (V1 with shorthand duration)
curl "http://localhost:3000/signalk/v1/history/values?duration=1h&paths=navigation.speedOverGround:sma:5"

# EMA with alpha of 0.3 - returns ONLY the smoothed value (V1 with shorthand duration)
curl "http://localhost:3000/signalk/v1/history/values?duration=1h&paths=environment.wind.speedApparent:ema:0.3"
```

#### Extension Parameters (non-standard)

| Parameter | Description | Format | Examples |
|-----------|-------------|---------|----------|
| `paths` ⚠️ | Extended smoothing syntax: `path:method:smoothing:param` (returns raw AND smoothed) | Extended format | `navigation.speedOverGround:average:sma:5` |
| `includeMovingAverages` ⚠️ | Include EMA/SMA calculations | `true` or `1` | `includeMovingAverages=true` |
| `bbox` ⚠️ | Bounding box filter: `west,south,east,north` | Coordinates | `bbox=-74.5,40.2,-73.8,40.9` |
| `radius` ⚠️ | Radius filter: `lon,lat,meters` (GeoJSON convention) | Coordinates + meters | `radius=-73.981,40.646,100` |

> ⚠️ **Extensions**: Parameters marked with ⚠️ are non-standard extensions to the SignalK History API specification. They provide additional functionality but may not be supported by other SignalK history providers.

### Query Examples

#### Pattern 1: Duration Only (Query back from now)
```bash
# Last hour of wind data
curl "http://localhost:3000/signalk/v1/history/values?duration=1h&paths=environment.wind.speedApparent"

# Last 30 minutes with moving averages
curl "http://localhost:3000/signalk/v1/history/values?duration=30m&paths=environment.wind.speedApparent&includeMovingAverages=true"

```

#### Pattern 2: From + Duration (Query forward)
```bash
# 6 hours forward from specific time
curl "http://localhost:3000/signalk/v1/history/values?from=2025-01-01T00:00:00Z&duration=6h&paths=navigation.position"
```

#### Pattern 3: To + Duration (Query backward)
```bash
# 2 hours backward to specific time
curl "http://localhost:3000/signalk/v1/history/values?to=2025-01-01T12:00:00Z&duration=2h&paths=environment.wind.speedApparent"
```

#### Pattern 4: From Only (From start to now)
```bash
# From specific time until now
curl "http://localhost:3000/signalk/v1/history/values?from=2025-01-01T00:00:00Z&paths=navigation.speedOverGround"
```

#### Pattern 5: From + To (Specific range)
```bash
# Specific 24-hour period
curl "http://localhost:3000/signalk/v1/history/values?from=2025-01-01T00:00:00Z&to=2025-01-02T00:00:00Z&paths=navigation.position"
```

#### Advanced Query Examples

**Multiple paths with time alignment:**
```bash
curl "http://localhost:3000/signalk/v1/history/values?duration=6h&paths=environment.wind.angleApparent,environment.wind.speedApparent,navigation.position&resolution=1m"
```

**Multiple aggregations of same path:**
```bash
curl "http://localhost:3000/signalk/v1/history/values?from=2025-01-01T00:00:00Z&to=2025-01-01T06:00:00Z&paths=environment.wind.speedApparent:average,environment.wind.speedApparent:min,environment.wind.speedApparent:max&resolution=60"
```

**With moving averages for trend analysis:**
```bash
curl "http://localhost:3000/signalk/v1/history/values?duration=24h&paths=electrical.batteries.512.voltage&includeMovingAverages=true&resolution=5m"
```

**Different temporal samples:**
```bash
curl "http://localhost:3000/signalk/v1/history/values?duration=1h&paths=navigation.position:first,navigation.position:middle_index,navigation.position:last&resolution=1m"
```

**Using ISO 8601 duration format:**
```bash
curl "http://localhost:3000/signalk/v1/history/values?duration=PT1H30M&paths=navigation.speedOverGround&resolution=30"
```

**Using integer seconds for duration:**
```bash
curl "http://localhost:3000/signalk/v1/history/values?duration=3600&paths=navigation.speedOverGround&resolution=10s"
```

#### Context and Path Discovery

**Get contexts with data in last hour:**
```bash
curl "http://localhost:3000/signalk/v1/history/contexts?duration=1h"
```

**Get contexts for specific time range:**
```bash
curl "http://localhost:3000/signalk/v1/history/contexts?from=2025-01-01T00:00:00Z&to=2025-01-07T00:00:00Z"
```

**Get available paths with recent data:**
```bash
curl "http://localhost:3000/signalk/v1/history/paths?duration=24h"
```

**Get all paths (no time filter):**
```bash
curl "http://localhost:3000/signalk/v1/history/paths"
```

#### Duration Formats
- `30s` - 30 seconds
- `15m` - 15 minutes
- `2h` - 2 hours
- `1d` - 1 day

#### Spatial Filtering (NEW)

Filter data by geographic location using bounding boxes or radius queries:

**Bounding Box Filter:**
```bash
# Position data within a bounding box (west,south,east,north)
curl "http://localhost:3000/signalk/v1/history/values?duration=1h&paths=navigation.position&bbox=-74.5,40.2,-73.8,40.9"
```

**Radius Filter:**
```bash
# Position data within 100m of a point (lon,lat,meters — GeoJSON convention)
curl "http://localhost:3000/signalk/v1/history/values?duration=1h&paths=navigation.position&radius=-73.981,40.646,100"
```

**Spatial Correlation (filter non-position paths by location):**
```bash
# Wind data when vessel was within 100m of point
curl "http://localhost:3000/signalk/v1/history/values?duration=24h&paths=environment.wind.speedApparent&radius=-73.981,40.646,100"

# Multiple paths filtered by bounding box
curl "http://localhost:3000/signalk/v1/history/values?duration=7d&paths=environment.wind.speedApparent,environment.depth.belowKeel&bbox=-74.0,40.6,-73.9,40.7"

```

**How Spatial Correlation Works:**
- For **position paths** (e.g., `navigation.position`): Filters directly on lat/lon
- For **non-position paths** (e.g., `environment.wind.speedApparent`): First queries position data to find timestamps when vessel was within the spatial filter, then returns only data from those times
- Spatial correlation always uses `navigation.position` for location lookup

#### Query Data Hierarchy

Queries resolve data from a three-tier hierarchy, selected automatically based on date ranges:

1. **Local tiered Parquet** — Primary source. Uses aggregation tiers (`raw`, `5s`, `60s`, `1h`) based on query resolution. Data available from first export through yesterday.
2. **Cloud supplement (S3/R2)** — For dates before local data starts (older than `retentionDays`). DuckDB queries cloud storage directly without downloading files first.
3. **SQLite buffer** — Today's live data, not yet exported to Parquet. Bucketed to match the query resolution.

For queries spanning multiple sources, results are combined with UNION.

**Cloud Query Optimization:**
DuckDB's native cloud storage support provides:
- **Partition pruning**: Hive structure (`year=/day=`) allows skipping irrelevant files
- **Predicate pushdown**: WHERE clauses filter at Parquet level before transfer
- **Projection pushdown**: Only SELECT columns are transferred
- **Combined effect**: 70-99% reduction vs downloading full files

**Requirements for cloud queries:**
- Cloud upload must be configured (`provider: 's3'` or `'r2'`)
- Valid credentials configured
- Data must be uploaded using Hive partition structure

### Timestamp Handling

All timestamps follow ISO 8601 conventions:
- **Bare timestamps** (e.g., `2025-08-13T09:00:00`) are treated as server local time
- **Z-suffix** (e.g., `2025-08-13T09:00:00Z`) is UTC
- **Explicit offset** (e.g., `2025-08-13T09:00:00-04:00`) is parsed as-is

All response timestamps are in server local time with offset (e.g., `2025-10-20T12:34:04-04:00`).

```bash
# Bare timestamp — interpreted as server local time
curl "http://localhost:3000/signalk/v1/history/values?context=vessels.self&to=2025-08-13T09:00:00&duration=1h&paths=navigation.position"

# Explicit UTC
curl "http://localhost:3000/signalk/v1/history/values?context=vessels.self&to=2025-08-13T09:00:00Z&duration=1h&paths=navigation.position"

# Explicit offset
curl "http://localhost:3000/signalk/v1/history/values?context=vessels.self&to=2025-08-13T09:00:00-04:00&duration=1h&paths=navigation.position"
```

**Get available contexts:**
```bash
curl "http://localhost:3000/signalk/v1/history/contexts"
```

### Time Alignment and Bucketing

The History API automatically aligns data from different paths using time bucketing to solve the common problem of misaligned timestamps. This enables:

- **Plotting**: Data points align properly on charts  
- **Correlation**: Compare values from different sensors at the same time
- **Export**: Clean, aligned datasets for analysis

**Key Features:**
- **Smart Type Handling**: Automatically handles numeric values (wind speed) and JSON objects (position)
- **Robust Aggregation**: Uses proper SQL type casting to prevent type errors
- **Configurable Resolution**: Time bucket size in seconds (default: auto-calculated based on time range)
- **Multiple Aggregation Methods**: `average` for numeric data, `first` for complex objects

**Parameters:**
- `resolution` - Time bucket size in seconds (default: auto-calculated)
- **Aggregation methods**: `average`, `min`, `max`, `first`, `last`, `mid`, `middle_index`

**Aggregation Methods:**
- **`average`** - Average value in time bucket (default for numeric data)
- **`min`** - Minimum value in time bucket
- **`max`** - Maximum value in time bucket
- **`first`** - First value in time bucket (default for objects)
- **`last`** - Last value in time bucket
- **`mid`** ⚠️ - Median value (average of middle values for even counts) - *extension*
- **`middle_index`** ⚠️ - Middle value by index (first of two middle values for even counts) - *extension*

**When to Use Each Method:**
- **Numeric data** (wind speed, voltage, etc.): Use `average`, `min`, `max` for statistics
- **Position data**: Use `first`, `last`, `middle_index` for specific readings
- **String/object data**: Avoid `mid` (unpredictable), prefer `first`, `last`, `middle_index`
- **Multiple stats**: Query same path with different methods (e.g., `wind:average,wind:max`)

### Response Format

The History API returns time-aligned data in standard SignalK format.

#### Default Response (without moving averages)

```json
{
  "context": "vessels.self",
  "range": {
    "from": "2025-01-01T00:00:00Z",
    "to": "2025-01-01T06:00:00Z"
  },
  "values": [
    {
      "path": "environment.wind.speedApparent",
      "method": "average"
    },
    {
      "path": "navigation.position",
      "method": "first"
    }
  ],
  "data": [
    ["2025-01-01T00:00:00Z", 12.5, {"latitude": 37.7749, "longitude": -122.4194}],
    ["2025-01-01T00:01:00Z", 13.2, {"latitude": 37.7750, "longitude": -122.4195}],
    ["2025-01-01T00:02:00Z", 11.8, {"latitude": 37.7751, "longitude": -122.4196}]
  ]
}
```

#### With Moving Averages (includeMovingAverages=true)

```json
{
  "context": "vessels.self",
  "range": {
    "from": "2025-01-01T00:00:00Z",
    "to": "2025-01-01T06:00:00Z"
  },
  "values": [
    {
      "path": "environment.wind.speedApparent",
      "method": "average"
    },
    {
      "path": "environment.wind.speedApparent.ema",
      "method": "ema"
    },
    {
      "path": "environment.wind.speedApparent.sma",
      "method": "sma"
    },
    {
      "path": "navigation.position",
      "method": "first"
    }
  ],
  "data": [
    ["2025-01-01T00:00:00Z", 12.5, 12.5, 12.5, {"latitude": 37.7749, "longitude": -122.4194}],
    ["2025-01-01T00:01:00Z", 13.2, 12.64, 12.85, {"latitude": 37.7750, "longitude": -122.4195}],
    ["2025-01-01T00:02:00Z", 11.8, 12.45, 12.5, {"latitude": 37.7751, "longitude": -122.4196}]
  ]
}
```

**Notes**:
- Each data array element is `[timestamp, value1, value2, ...]` corresponding to the paths in the `values` array
- Moving averages (EMA/SMA) are **opt-in** - add `includeMovingAverages=true` to include them
- EMA/SMA are only calculated for numeric values; non-numeric values (objects, strings) show `null` for their EMA/SMA columns
- Without `includeMovingAverages`, response size is ~66% smaller

#### Response Extensions (non-standard) ⚠️

When using extension parameters, the response may include additional non-standard fields:

| Field | Added by | Description |
|-------|----------|-------------|
| `meta.autoConfigured` | Auto-discovery | Indicates paths were auto-configured for recording |

These fields are extensions and may not be present in responses from other SignalK history providers.


## Moving Averages (EMA & SMA)

The plugin calculates **Exponential Moving Average (EMA)** and **Simple Moving Average (SMA)** for numeric values, providing enhanced trend analysis capabilities. There are two ways to enable smoothing:

### Per-Path Smoothing Syntax (NEW in v0.6.5)

Apply smoothing directly in the path specification using the `path:method:smoothing:param` syntax:

```bash
# SMA with 5-point window
curl "http://localhost:3000/signalk/v1/history/values?duration=1h&paths=navigation.speedOverGround:average:sma:5"

# EMA with alpha=0.3
curl "http://localhost:3000/signalk/v1/history/values?duration=1h&paths=environment.wind.speedApparent:max:ema:0.3"

# Mixed: some paths with smoothing, some without
curl "http://localhost:3000/signalk/v1/history/values?duration=1h&paths=navigation.speedOverGround:average:sma:5,navigation.courseOverGround:average"

# SMA with default period (10)
curl "http://localhost:3000/signalk/v1/history/values?duration=1h&paths=navigation.speedOverGround:average:sma"

# EMA with default alpha (0.2)
curl "http://localhost:3000/signalk/v1/history/values?duration=1h&paths=navigation.speedOverGround:average:ema"
```

**Path Syntax Format:** `path:aggregateMethod:smoothingType:smoothingParam`
- `path` - SignalK path (e.g., `navigation.speedOverGround`)
- `aggregateMethod` - Aggregation method: `average`, `min`, `max`, `first`, `last`, `mid` (default: `average`)
- `smoothingType` - `sma` (Simple Moving Average) or `ema` (Exponential Moving Average)
- `smoothingParam` - For SMA: window size (default: 10), for EMA: alpha value 0-1 (default: 0.2)

**Per-Path Response Format:**
```json
{
  "values": [
    {
      "path": "navigation.speedOverGround",
      "method": "average"
    },
    {
      "path": "navigation.speedOverGround",
      "method": "average",
      "smoothing": "sma",
      "window": 5
    }
  ],
  "data": [
    ["2025-01-01T00:00:00Z", 5.05, 5.05],
    ["2025-01-01T00:01:00Z", 5.12, 5.09],
    ["2025-01-01T00:02:00Z", 4.98, 5.05]
  ]
}
```

**Note:** With per-path smoothing, the response includes both the raw value AND the smoothed value as separate columns. Paths without smoothing specified only get a single column.

### Global Moving Averages (Legacy)

**History API:**
```bash
# Add includeMovingAverages=true to any query
curl "http://localhost:3000/signalk/v1/history/values?duration=1h&paths=environment.wind.speedApparent&includeMovingAverages=true"
```

**Default Behavior (v0.5.6+):**
- Moving averages are **opt-in** - not included by default
- Reduces response size by ~66% when not needed
- Better API compliance with SignalK specification

**Legacy Behavior (pre-v0.5.6):**
- Moving averages were automatically included for all queries
- To maintain old behavior, add `includeMovingAverages=true` to all requests

### Calculation Details

#### Exponential Moving Average (EMA)
- **Period**: ~10 equivalent (α = 0.2)
- **Formula**: `EMA = α × currentValue + (1 - α) × previousEMA`
- **Characteristic**: Responds faster to recent changes, emphasizes recent data
- **Use Case**: Trend detection, rapid response to data changes

#### Simple Moving Average (SMA)
- **Period**: 10 data points
- **Formula**: Average of the last 10 values
- **Characteristic**: Smooths out fluctuations, equal weight to all values in window
- **Use Case**: Noise reduction, general trend analysis

### Data Flow & Continuity

```javascript
// Initial Data Load (isIncremental: false)
Point 1: Value=5.0, EMA=5.0,   SMA=5.0
Point 2: Value=6.0, EMA=5.2,   SMA=5.5
Point 3: Value=4.0, EMA=5.0,   SMA=5.0

// Incremental Updates (isIncremental: true)
Point 4: Value=7.0, EMA=5.4,   SMA=5.5  // Continues from previous EMA
Point 5: Value=5.5, EMA=5.42,  SMA=5.5  // Rolling 10-point SMA window
```

### Key Features

- 🎛️ **Opt-In**: Add `includeMovingAverages=true` to enable (v0.5.6+)
- ✅ **Memory Efficient**: SMA maintains rolling 10-point window
- ✅ **Non-Numeric Handling**: Non-numeric values (strings, objects) show `null` for EMA/SMA
- ✅ **Precision**: Values rounded to 3 decimal places to prevent floating-point noise
- ⚡ **Performance**: Smaller response sizes when not needed

### Real-world Applications

**Marine Data Examples:**
- **Wind Speed**: EMA detects gusts quickly, SMA shows general wind conditions
- **Battery Voltage**: EMA shows charging/discharging trends, SMA indicates overall battery health
- **Engine RPM**: EMA responds to throttle changes, SMA shows average operating level
- **Water Temperature**: EMA detects thermal changes, SMA provides stable baseline

**Available in:**
- 📊 **History API**: Add `includeMovingAverages=true` to include EMA/SMA calculations


## Cloud Storage (S3 / Cloudflare R2)

### Configuration

Set `provider` to `s3` or `r2` in the cloud upload configuration. Cloud uploads run automatically as part of the daily export pipeline — after Parquet files are created, they are uploaded to the configured cloud provider.

```json
{
  "cloudUpload": {
    "provider": "s3",
    "bucket": "my-marine-data",
    "region": "us-east-1",
    "keyPrefix": "marine-data/",
    "accessKeyId": "...",
    "secretAccessKey": "..."
  }
}
```

For Cloudflare R2, use `provider: "r2"` and supply `accountId` instead of `region`.

### Cloud Key Structure

With prefix `marine-data/` and Hive partitioning:
```
marine-data/tier=raw/context=vessels__self/path=navigation__position/year=2026/day=062/signalk_data_2026-03-03T0400.parquet
```

## Daily Export

The plugin uses a simplified daily export pipeline:

1. **Data Collection**: SignalK data is buffered in crash-safe SQLite WAL database
2. **Daily Export**: At configurable hour (default: 4 AM UTC), exports previous day's data
3. **Direct Consolidation**: Creates one Parquet file per context/path/day (no separate merge step)
4. **Timestamped Files**: Each export uses current timestamp for unique filenames
5. **S3 Upload**: Uploads daily files if configured

**Note**: The previous 5-minute interval export and separate consolidation step have been removed. This simplifies the pipeline and creates cleaner daily files directly.

## Startup Sequence

When the plugin starts, it runs the following initialization steps:

1. **Configuration & State** — Load plugin config, vessel identity, output directory, cloud credentials
2. **SQLite Buffer** — Open WAL-mode database; auto-migrate legacy `buffer_records` table to per-path tables if needed
3. **Cloud Client** — Initialize S3 or R2 SDK if a cloud provider is configured
4. **DuckDB Pool** — Initialize connection pool; attach SQLite buffer for federated queries; register cloud credentials
5. **Data Subscriptions** — Subscribe to configured SignalK paths and start threshold monitoring
6. **Periodic Save** — Start flush interval (default: every 30s) from memory buffer to SQLite
7. **Daily Export Schedule** — Schedule next export at configured UTC hour (default: 4 AM); includes aggregation and cloud upload
8. **Startup Catch-Up** (10s delay) — Export any unexported historical data from SQLite, re-aggregate affected dates, and sync recent files to cloud (7-day lookback, raw-tier-only prefix scan)
9. **History API** — Register HTTP routes and SignalK HistoryApi provider
10. **Auto-Discovery** — Initialize service for on-demand path configuration

## Performance Characteristics

- **Memory Usage**: Configurable buffer sizes (default 1000 records)
- **Disk I/O**: Efficient batch writes with configurable intervals
- **CPU Usage**: Minimal - mostly I/O bound operations
- **Network**: Optional S3 uploads with retry logic

## Development

### Project Structure

```
signalk-parquet/
├── src/
│   ├── index.ts                # Main plugin entry point and lifecycle
│   ├── commands.ts             # Command management system
│   ├── data-handler.ts         # Data processing, subscriptions, cloud upload
│   ├── api-routes.ts           # Web API endpoints
│   ├── types.ts                # TypeScript interfaces
│   ├── parquet-writer.ts       # File writing logic
│   ├── HistoryAPI.ts           # SignalK History API implementation
│   ├── HistoryAPI-types.ts     # History API type definitions
│   ├── history-provider.ts     # SignalK HistoryApi provider (v2)
│   ├── historical-streaming.ts # WebSocket historical data streaming (not yet functional)
│   ├── services/
│   │   ├── aggregation-service.ts  # Tier aggregation (raw→5s→60s→1h)
│   │   └── parquet-export-service.ts # Daily export pipeline
│   └── utils/
│       ├── sqlite-buffer.ts    # Per-path SQLite buffer
│       ├── buffer-sql-builder.ts # SQL generation for buffer queries
│       ├── duckdb-pool.ts      # DuckDB connection pooling
│       ├── hive-path-builder.ts # Hive partition path generation
│       ├── angular-paths.ts    # Angular path detection (units=rad)
│       ├── duration-parser.ts  # ISO 8601 and shorthand duration parsing
│       ├── spatial-queries.ts  # Geographic/spatial query support
│       ├── lru-cache.ts        # LRU cache implementation
│       ├── path-helpers.ts     # Path utility functions
│       ├── path-discovery.ts   # Path auto-discovery
│       ├── context-discovery.ts # Context auto-discovery
│       ├── type-detector.ts    # Smart data type detection
│       ├── schema-cache.ts     # Parquet schema caching
│       └── ...                 # Additional utilities
├── dist/                       # Compiled JavaScript
├── public/
│   ├── index.html             # Web interface
│   └── parquet.png            # Plugin icon
├── tsconfig.json              # TypeScript configuration
├── package.json               # Dependencies and scripts
└── README.md                  # This file
```

### Code Architecture

The plugin uses a modular TypeScript architecture for maintainability:

- **`index.ts`**: Plugin lifecycle, configuration, and initialization
- **`commands.ts`**: SignalK command registration, execution, and management
- **`data-handler.ts`**: Data subscriptions, buffering, and cloud upload operations
- **`api-routes.ts`**: REST API endpoints for web interface
- **`historical-streaming.ts`**: WebSocket historical streaming (not yet functional)
- **`types.ts`**: Comprehensive TypeScript type definitions
- **`services/`**: Aggregation and export pipeline services
- **`utils/`**: SQLite buffer, DuckDB pool, spatial queries, path helpers, and more

### Adding New Features

1. **API Endpoints**: Add to `src/api-routes.ts`
2. **Data Processing**: Extend `src/data-handler.ts`
3. **Commands**: Modify `src/commands.ts`
4. **Types**: Add interfaces to `src/types.ts`
5. **Update Documentation**: Update README and inline comments

### Type Checking

The plugin uses strict TypeScript configuration:

```json
{
  "compilerOptions": {
    "strict": true,
    "noImplicitAny": true,
    "noImplicitReturns": true,
    "strictNullChecks": true
  }
}
```

## Troubleshooting

### Common Issues

**Build Errors**
```bash
# Clean and rebuild
npm run clean
npm run build
```

**DuckDB Not Available**
- Check that `@duckdb/node-api` is installed
- Verify Node.js version compatibility (>=22.5.0)

**Cloud Upload Failures**
- Verify cloud credentials and permissions
- Check bucket exists and is accessible
- Test connection using web interface

**No Data Collection**
- Verify path configurations are correct
- Check if regimens are properly activated
- Review SignalK logs for subscription errors

### Debug Mode

Enable debug logging in SignalK:

```json
{
  "settings": {
    "debug": "signalk-parquet*"
  }
}
```


### Runtime Dependencies

- `@dsnp/parquetjs`: Parquet file format support
- `@duckdb/node-api`: SQL query engine
- `@aws-sdk/client-s3`: S3 upload functionality
- `fs-extra`: Enhanced file system operations
- `glob`: File pattern matching
- `express`: Web server framework

### Development Dependencies

- `typescript`: TypeScript compiler
- `@types/node`: Node.js type definitions
- `@types/express`: Express type definitions
- `@types/fs-extra`: fs-extra type definitions

## License

MIT License - See LICENSE file for details.


## Testing

Comprehensive testing procedures are documented in `TESTING.md`. The testing guide covers:

- Installation and build verification
- Plugin configuration testing
- Web interface functionality
- Data collection validation
- Regimen control testing
- File output verification
- Cloud storage integration testing
- API endpoint testing
- Performance testing
- Error handling validation

### Quick Test

```bash
# Test plugin health
curl http://localhost:3000/plugins/signalk-parquet/api/health

# Test path configuration
curl http://localhost:3000/plugins/signalk-parquet/api/config/paths

# Test data collection
curl http://localhost:3000/plugins/signalk-parquet/api/paths

# Test History API
curl "http://localhost:3000/signalk/v1/history/contexts"
```

## Legacy Notes

### Upgrading from 0.5.0-beta.3: Consolidation Bug Fix

If upgrading from versions prior to 0.5.0-beta.4, you may have nested `processed` directories from a recursive consolidation bug:

```bash
# Check for nested processed directories
find data -name "*processed*" -type d | head -20

# Remove ALL nested processed directories (RECOMMENDED)
find data -name "processed" -type d -exec rm -rf {} +
```

The `processed` directories contain legacy files from the old consolidation system. The new daily export pipeline no longer uses this directory.

### Legacy Flat File Structure

Pre-Hive versions stored data in a flat directory structure:
```
output_directory/
├── vessels/
│   └── self/
│       ├── navigation/
│       │   └── position/
│       │       └── signalk_data_20250716T120000.parquet
└── processed/
```

Use the Migration API to convert legacy files to Hive partitioning.

## TODO

- [x] Implement daily export pipeline (replaced consolidation)
- [x] Add history API integration
- [x] S3 federated querying with DuckDB
- [x] Spatial correlation for non-position paths
- [x] SQLite WAL buffering for crash-safe data ingestion
- [x] Hive-partitioned storage with aggregation tiers
- [x] Migration service for flat-to-Hive conversion
- [x] Auto-discovery for on-demand path configuration
- [ ] Incorporate user preferences from units-preference in the regimen filter system
- [ ] Expose recorded spatial event via api endpoint (geojson)
- [ ] Add Grafana integration
- [ ] Dedicated WebSocket endpoint for historical data streaming (current delta interceptor approach doesn't work — needs a separate `ws://` endpoint that streams historical data on demand)

## Contributing

1. Fork the repository
2. Create a feature branch
3. Add TypeScript types for new features
4. Include tests and documentation
5. Follow the testing procedures in `TESTING.md`
6. Submit a pull request

## Changelog

See [CHANGELOG.md](CHANGELOG.md) for complete version history.
