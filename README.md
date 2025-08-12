# SignalK Parquet Data Store

**Version 0.5.0-beta.6**

A comprehensive TypeScript-based SignalK plugin that saves marine data directly to Parquet files with regimen-based control, web interface for querying, and S3 upload capabilities.

## Features

- **Smart Data Types**: Intelligent Parquet schema detection preserves native data types (DOUBLE, BOOLEAN) instead of forcing everything to strings
- **Command Management**: Register, execute, and manage SignalK commands with automatic path configuration
- **Regimen-Based Data Collection**: Control data collection with command-based regimens
- **Multi-Vessel Support**: Wildcard vessel contexts (`vessels.*`) with MMSI-based exclusion filtering
- **Multiple File Formats**: Support for Parquet, JSON, and CSV output formats (querying in parquet only)
- **Web Interface**: Responsive web interface for data exploration and configuration
- **DuckDB Integration**: Query Parquet files directly with SQL
- **History API Integration**: Full SignalK History API implementation for historical data queries
- **S3 Integration**: Upload files to Amazon S3 with configurable timing
- **Daily Consolidation**: Automatic daily file consolidation
- **Real-time Buffering**: Efficient data buffering with configurable thresholds
- **Source Filtering**: Filter data by SignalK source labels (bypasses server arbitration for raw data access)
- **Context Support**: Support for multiple vessel contexts with exclusion controls

## Installation

### Install from GitHub
```bash
# Navigate to folder
cd ~/.signalk/node_modules/

# Install from npm (after publishing)
npm install signalk-parquet

# Or install from GitHub
npm install motamman/signalk-parquet

# Restart SignalK
sudo systemctl restart signalk
```

## ⚠️ IMPORTANT: Consolidation Bug Fix

**THIS VERSION FIXES A RECURSIVE BUG THAT WAS CREATING NESTED PROCESSED DIRECTORIES AND REPEATEDLY PROCESSING THE SAME FILES. THIS SHOULD FIX THAT PROBLEM BUT ANY `processed` FOLDERS NESTED INSIDE A `processed` FOLDER SHOULD BE MANUALLY DELETED.**

### Cleaning Up Nested Processed Directories

If you're upgrading from a previous version, you may have nested processed directories that need cleanup:

```bash
# Check for nested processed directories
find data -name "*processed*" -type d | head -20

# See the deepest nesting levels
find data -name "*processed*" -type d | awk -F'/' '{print NF-1, $0}' | sort -nr | head -5

# Count files in nested processed directories
find data -path "*/processed/processed/*" -type f | wc -l

# Remove ALL nested processed directories (RECOMMENDED)
find data -name "processed" -type d -exec rm -rf {} +

# Verify cleanup completed
find data -path "*/processed/processed/*" -type f | wc -l  # Should show 0
```

**Note**: The processed directories only contain files that were moved during consolidation - removing them does not delete your original data.

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

## Development Scripts

- `npm run build` - Compile TypeScript to JavaScript
- `npm run watch` - Watch for changes and recompile
- `npm run clean` - Remove compiled files
- `npm run dev` - Build and watch for changes

## 

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

### S3 Upload Configuration

Configure S3 upload settings in the plugin configuration:

| Setting | Description | Default |
|---------|-------------|---------|
| **Enable S3 Upload** | Enable uploading to Amazon S3 | `false` |
| **Upload Timing** | When to upload (realtime/consolidation) | `consolidation` |
| **S3 Bucket** | Name of S3 bucket | - |
| **AWS Region** | AWS region for S3 bucket | `us-east-1` |
| **Key Prefix** | S3 object key prefix | - |
| **Access Key ID** | AWS credentials (optional) | - |
| **Secret Access Key** | AWS credentials (optional) | - |
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

The plugin now streamlines command management with automatic path configuration:

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

**Note**: Source filtering accesses raw data streams before SignalK server arbitration, allowing collection of data from specific sources that might otherwise be filtered out.

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
  paths: PathConfig[];
  s3Upload: S3UploadConfig;
}

interface PathConfig {
  path: string;
  enabled: boolean;
  regimen?: string;
  source?: string;
  context: string;
  excludeMMSI?: string[];
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
  dataBuffers: Map<string, DataRecord[]>;
  activeRegimens: Set<string>;
  subscribedPaths: Set<string>;
  parquetWriter?: ParquetWriter;
  s3Client?: any;
  currentConfig?: PluginConfig;
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

### File Organization

```
output_directory/
├── vessels/
│   └── self/
│       ├── navigation/
│       │   ├── position/
│       │   │   ├── signalk_data_20250716T120000.parquet
│       │   │   └── signalk_data_20250716_consolidated.parquet
│       │   └── speedOverGround/
│       └── environment/
│           └── wind/
│               └── angleApparent/
└── processed/
    └── [moved files after consolidation]
```

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
- **S3 Status**: Test S3 connectivity and configuration
- **Responsive Design**: Works on desktop and mobile
- **MMSI Filtering**: Exclude specific vessels from wildcard contexts

### API Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/paths` | GET | List available data paths |
| `/api/files/:path` | GET | List files for a path |
| `/api/sample/:path` | GET | Sample data from a path |
| `/api/query` | POST | Execute SQL query |
| `/api/config/paths` | GET/POST/PUT/DELETE | Manage path configurations |
| `/api/test-s3` | POST | Test S3 connection |
| `/api/health` | GET | Health check |
| `/signalk/v1/history/values` | GET | SignalK History API - Get historical values |
| `/signalk/v1/history/contexts` | GET | SignalK History API - Get available contexts |
| `/signalk/v1/history/paths` | GET | SignalK History API - Get available paths |

## DuckDB Integration

### Query Examples

```sql
-- Get latest 10 records from navigation position
SELECT * FROM '/path/to/navigation/position/*.parquet' 
ORDER BY received_timestamp DESC LIMIT 10;

-- Count total records
SELECT COUNT(*) FROM '/path/to/navigation/position/*.parquet';

-- Filter by source
SELECT * FROM '/path/to/environment/wind/*.parquet' 
WHERE source_label = 'mqtt-weatherflow-udp'
ORDER BY received_timestamp DESC LIMIT 100;

-- Aggregate by hour
SELECT 
  DATE_TRUNC('hour', received_timestamp::timestamp) as hour,
  AVG(value::double) as avg_value,
  COUNT(*) as record_count
FROM '/path/to/data/*.parquet'
GROUP BY hour
ORDER BY hour;
```

## History API Integration

The plugin provides full SignalK History API compatibility, allowing you to query historical data using standard SignalK API endpoints.

### Available Endpoints

| Endpoint | Description | Parameters |
|----------|-------------|------------|
| `/signalk/v1/history/values` | Get historical values for specified paths | `context`, `from`, `to`, `paths` |
| `/signalk/v1/history/contexts` | Get available vessel contexts | `from`, `to` (optional) |
| `/signalk/v1/history/paths` | Get available SignalK paths | `from`, `to` (optional) |

### Query Examples

**Get historical position data:**
```bash
curl "http://localhost:3000/signalk/v1/history/values?context=vessels.self&from=2025-01-01T00:00:00Z&to=2025-01-02T00:00:00Z&paths=navigation.position"
```

**Get wind data with multiple paths:**
```bash
curl "http://localhost:3000/signalk/v1/history/values?context=vessels.self&from=2025-01-01T00:00:00Z&to=2025-01-01T06:00:00Z&paths=environment.wind.angleApparent,environment.wind.speedApparent"
```

**Get available contexts:**
```bash
curl "http://localhost:3000/signalk/v1/history/contexts"
```

### Response Format

The History API returns data in standard SignalK format:

```json
{
  "context": "vessels.self",
  "data": [
    {
      "timestamp": "2025-01-01T12:00:00.000Z",
      "values": [
        {
          "path": "navigation.position",
          "value": {
            "latitude": 37.7749,
            "longitude": -122.4194
          }
        }
      ]
    }
  ]
}
```

## S3 Integration

### Upload Timing

**Real-time Upload**: Files are uploaded immediately after creation
```json
{
  "s3Upload": {
    "enabled": true,
    "timing": "realtime"
  }
}
```

**Consolidation Upload**: Files are uploaded after daily consolidation
```json
{
  "s3Upload": {
    "enabled": true,
    "timing": "consolidation"
  }
}
```

### S3 Key Structure

With prefix `marine-data/`:
```
marine-data/vessels/self/navigation/position/signalk_data_20250716_consolidated.parquet
marine-data/vessels/self/environment/wind/angleApparent/signalk_data_20250716_120000.parquet
```

## File Consolidation

The plugin automatically consolidates files daily at midnight UTC:

1. **File Discovery**: Finds all files for the previous day
2. **Merging**: Combines files by SignalK path
3. **Sorting**: Sorts records by timestamp
4. **Cleanup**: Moves source files to `processed/` directory
5. **S3 Upload**: Uploads consolidated files if configured

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
│   ├── index.ts              # Main plugin logic
│   ├── types.ts              # TypeScript interfaces
│   ├── parquet-writer.ts     # File writing logic
│   ├── HistoryAPI.ts         # SignalK History API implementation
│   └── HistoryAPI-types.ts   # History API type definitions
├── dist/                     # Compiled JavaScript
├── public/
│   ├── index.html           # Web interface
│   └── parquet.png          # Plugin icon
├── tsconfig.json            # TypeScript configuration
├── package.json             # Dependencies and scripts
└── README.md               # This file
```

### Adding New Features

1. **Update Types**: Add interfaces to `src/types.ts`
2. **Implement Logic**: Add functionality to appropriate files
3. **Add Tests**: Create unit tests for new features
4. **Update Documentation**: Update README and inline comments

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
- Verify Node.js version compatibility (>=16.0.0)

**S3 Upload Failures**
- Verify AWS credentials and permissions
- Check S3 bucket exists and is accessible
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
- S3 integration testing
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

For detailed testing procedures, see [TESTING.md](TESTING.md).

## TODO

- [x] Implement startup consolidation for missed previous days (exclude current day)
- [x] Add history API integration
- [ ] Clean up data output of sourcing
- [ ] Add Grafana integration
- [ ] Create SignalK app store listing with screenshots

## Contributing

1. Fork the repository
2. Create a feature branch
3. Add TypeScript types for new features
4. Include tests and documentation
5. Follow the testing procedures in `TESTING.md`
6. Submit a pull request

## Changelog

### Version 0.5.0-beta.6
- **📊 History API Integration**: Implemented full SignalK History API compatibility with endpoints for `/signalk/v1/history/values`, `/signalk/v1/history/contexts`, and `/signalk/v1/history/paths`
- **🔍 Parquet File Queries**: Added robust querying of historical data from Parquet files using DuckDB integration
- **🧹 Code Quality**: Fixed linting and prettier formatting errors throughout the codebase
- **🏗️ Type Safety**: Enhanced type definitions with HistoryAPI-types.ts for better development experience

### Version 0.5.0-beta.5
- **🔧 Fixed BigInt Serialization**: Resolved BigInt serialization errors that prevented Parquet file consolidation from completing
- **🌩️ Fixed S3 Client Initialization**: Resolved timing issues where AWS SDK imports were not ready during plugin startup
- **📤 Smart S3 Upload**: Added timestamp-based conflict resolution - only uploads files newer than existing S3 versions
- **🚀 Automatic S3 Catch-up**: Added startup upload of existing consolidated files to S3 (catch-up mechanism)
- **🔍 Enhanced Debug Logging**: Added comprehensive debug logging for S3 operations and consolidation processes
- **🛡️ Improved Error Handling**: Enhanced error handling and logging throughout S3 upload pipeline

### Version 0.5.0-beta.4
- Previous release with consolidation improvements

### Version 0.5.0-beta.3
- **🔥 CRITICAL BUG FIX**: Fixed recursive consolidation bug that created infinite nested `/processed/processed/processed/...` directories
- **🎯 Smart Data Types**: Implemented intelligent Parquet schema detection that preserves native data types (DOUBLE, BOOLEAN, INT64) instead of forcing everything to UTF8 strings
- **⚡ Performance**: Numeric data now stored as native types for better compression and faster queries
- **🛠️ Type Safety**: Fixed schema field type access bug (`primitiveType` → `type`) in data conversion logic

### Version 0.5.0-beta.2
- **🐛 Fixed SignalK Source Naming**: Corrected delta updates to use `$source` instead of `source` object format for proper source label recognition
- **🧹 Reduced Debug Spam**: Removed redundant path subscription checks in delta processing
- **🔧 Enhanced Date Processing**: Updated date extraction logic and startup consolidation for missed previous days

### Version 0.5.0-beta.1
- Complete TypeScript rewrite with enhanced type safety
- **Source filtering with raw data access**: Bypass SignalK server arbitration for specific sources
- **Streambundle API integration**: Improved performance with app.streambundle instead of subscription manager
- **Backward compatibility**: Automatic config migration for older path configurations
- **Multi-vessel support**: `vessels.*` wildcard context with MMSI exclusion filtering
- **Enhanced web interface**: Restored source filtering UI with improved controls
- **Streamlined command management**: Automatic path configuration for commands
- **SignalK API compliance**: Proper subscription patterns for vessel contexts
- Performance optimizations and better error handling