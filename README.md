# SignalK Parquet Data Store (TypeScript)

**Version 0.5.0-alpha.2**

A comprehensive TypeScript-based SignalK plugin that saves marine data directly to Parquet files with regimen-based control, web interface for querying, and S3 upload capabilities.

## Features

- **TypeScript Implementation**: Full TypeScript support with comprehensive type safety
- **Regimen-Based Data Collection**: Control data collection with command-based regimens
- **Multiple File Formats**: Support for Parquet, JSON, and CSV output formats
- **Web Interface**: Beautiful, responsive web interface for data exploration
- **DuckDB Integration**: Query Parquet files directly with SQL
- **S3 Integration**: Upload files to Amazon S3 with configurable timing
- **Daily Consolidation**: Automatic daily file consolidation
- **Real-time Buffering**: Efficient data buffering with configurable thresholds
- **Source Filtering**: Filter data by SignalK source labels
- **Context Support**: Support for multiple vessel contexts

## Installation

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
   - **Context**: SignalK context (usually `vessels.self`)
3. Click **✅ Add Path**

### Managing Existing Paths

- **Edit Path**: Click ✏️ Edit button to modify path settings
- **Delete Path**: Click 🗑️ Remove button to delete a path
- **Refresh**: Click 🔄 Refresh Paths to reload configuration

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

**Example**: Weather data collection controlled by regimen
```json
{
  "path": "environment.wind.angleApparent",
  "enabled": false,
  "regimen": "captureWeather",
  "source": "mqtt-weatherflow-udp",
  "context": "vessels.self"
}
```

**Command Path**: Add the corresponding command path
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
| `value` | any | The actual data value |
| `value_json` | string | JSON representation for complex values |
| `source` | string | Complete source information |
| `source_label` | string | Source label |
| `source_type` | string | Source type |
| `source_pgn` | number | PGN number (if applicable) |
| `meta` | string | Metadata information |

## Web Interface

### Features

- **Path Configuration**: Manage data collection paths
- **Data Exploration**: Browse available data paths
- **SQL Queries**: Execute DuckDB queries against Parquet files
- **S3 Status**: Test S3 connectivity and configuration
- **Responsive Design**: Works on desktop and mobile

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
│   └── parquet-writer.ts     # File writing logic
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

## Migration from JavaScript Version

1. **Backup Configuration**: Save current plugin configuration
2. **Stop Plugin**: Disable the JavaScript version
3. **Install TypeScript Version**: Follow installation instructions
4. **Migrate Configuration**: Use same configuration parameters
5. **Verify Operation**: Check data collection and web interface

## Dependencies

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

## Support

For issues and feature requests:
- **Plugin Issues**: Report via GitHub issues
- **SignalK Integration**: Check SignalK documentation
- **TypeScript Issues**: Consult TypeScript documentation
- **DuckDB Issues**: Check DuckDB documentation

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
```

For detailed testing procedures, see [TESTING.md](TESTING.md).

## Contributing

1. Fork the repository
2. Create a feature branch
3. Add TypeScript types for new features
4. Include tests and documentation
5. Follow the testing procedures in `TESTING.md`
6. Submit a pull request

## Changelog

### Version 0.5.0-alpha.2
- Complete TypeScript rewrite
- Enhanced type safety
- Improved error handling
- Better documentation
- Updated web interface
- Performance optimizations