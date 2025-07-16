# SignalK Parquet Plugin - Testing Guide

**Version 0.5.0-alpha.2**

This document provides comprehensive testing procedures for the SignalK Parquet plugin to ensure all functionality works correctly after installation and configuration.

## Prerequisites

- SignalK Server running (version >=2.13.0)
- Node.js >=16.0.0
- Plugin installed via: `npm install motamman/signalk-parquet`

## Testing Overview

### Test Categories

1. **Installation & Build Tests**
2. **Plugin Configuration Tests**
3. **Web Interface Tests**
4. **Data Collection Tests**
5. **Regimen Control Tests**
6. **File Output Tests**
7. **S3 Integration Tests**
8. **API Endpoint Tests**
9. **Performance Tests**
10. **Error Handling Tests**

---

## 1. Installation & Build Tests

### Test 1.1: GitHub Installation
```bash
# Test installation from GitHub
npm install motamman/signalk-parquet

# Expected: No build errors, TypeScript compiles successfully
# Check: Plugin appears in SignalK admin plugins list
```

### Test 1.2: TypeScript Compilation
```bash
# Navigate to plugin directory
cd ~/.signalk/node_modules/signalk-parquet

# Test build
npm run build

# Expected: No TypeScript errors, dist/ directory created
# Check: dist/index.js, dist/types.d.ts exist
```

### Test 1.3: Plugin Loading
```bash
# Restart SignalK
sudo systemctl restart signalk

# Check SignalK logs for plugin loading
journalctl -u signalk -f | grep "signalk-parquet"

# Expected: "Initializing SignalK Parquet Plugin" message
# Expected: No loading errors or missing dependencies
```

---

## 2. Plugin Configuration Tests

### Test 2.1: Basic Configuration
1. Navigate to **SignalK Admin → Server → Plugin Config → SignalK Parquet Data Store**
2. Verify configuration options:
   - ✅ Buffer Size (default: 1000)
   - ✅ Save Interval (default: 30 seconds)
   - ✅ Output Directory
   - ✅ Filename Prefix (default: `signalk_data`)
   - ✅ File Format (parquet/json/csv)
   - ✅ Retention Days (default: 7)
   - ✅ S3 Upload Configuration
   - ❌ **Paths Configuration** (should NOT appear - only in web app)

### Test 2.2: Configuration Persistence
1. Change buffer size to 2000
2. Save configuration
3. Restart SignalK
4. Verify configuration persists

**Expected Result**: Path configuration is NOT visible in SignalK admin interface

---

## 3. Web Interface Tests

### Test 3.1: Web App Access
1. Navigate to: `http://localhost:3000/plugins/signalk-parquet`
2. Verify web interface loads with tabs:
   - ⚙️ Path Configuration
   - 📁 Available Data Paths
   - 🔍 Custom Query
   - ☁️ Cloud Status

### Test 3.2: Path Configuration Tab
1. Click **Path Configuration** tab
2. Test **Add New Path** functionality:
   ```
   SignalK Path: navigation.position
   Always Enabled: ✅
   Regimen Control: (empty)
   Source Filter: (empty)
   Context: vessels.self
   ```
3. Click **Add Path**
4. Verify path appears in configuration table

### Test 3.3: Path Management
1. **Edit Path**: Click ✏️ Edit on existing path
2. **Delete Path**: Click 🗑️ Remove on existing path
3. **Refresh**: Click 🔄 Refresh Paths
4. Verify all operations work correctly

### Test 3.4: Path Configuration Persistence
1. Add multiple paths via web interface
2. Restart SignalK
3. Check that paths persist in web interface
4. Verify configuration stored in: `~/.signalk/signalk-parquet/webapp-config.json`

---

## 4. Data Collection Tests

### Test 4.1: Basic Data Collection
1. Configure path: `navigation.position`
2. Enable in web interface
3. Wait 60 seconds
4. Check output directory for files:
   ```bash
   ls -la ~/.signalk/signalk-parquet/vessels/self/navigation/position/
   ```

### Test 4.2: Data File Verification
1. Verify files created with correct naming:
   - Format: `signalk_data_YYYYMMDDTHHMMSS.parquet`
   - Contains recent data
2. Check file contents using web interface query

### Test 4.3: Multi-Path Collection
1. Configure multiple paths:
   - `navigation.position`
   - `navigation.speedOverGround`
   - `environment.wind.angleApparent`
2. Verify separate directories created for each path
3. Check data collection for all paths

---

## 5. Regimen Control Tests

### Test 5.1: Regimen Configuration
1. Add path with regimen control:
   ```
   SignalK Path: environment.wind.angleApparent
   Always Enabled: ❌
   Regimen Control: captureWeather
   ```
2. Add command path:
   ```
   SignalK Path: commands.captureWeather
   Always Enabled: ✅
   ```

### Test 5.2: Regimen Activation
1. Send command via SignalK:
   ```bash
   curl -X POST http://localhost:3000/signalk/v1/api/vessels/self/commands/captureWeather \
     -H "Content-Type: application/json" \
     -d '{"value": true}'
   ```
2. Verify data collection starts for regimen-controlled paths

### Test 5.3: Regimen Deactivation
1. Send deactivation command:
   ```bash
   curl -X POST http://localhost:3000/signalk/v1/api/vessels/self/commands/captureWeather \
     -H "Content-Type: application/json" \
     -d '{"value": false}'
   ```
2. Verify data collection stops for regimen-controlled paths

---

## 6. File Output Tests

### Test 6.1: Parquet Format
1. Set file format to `parquet`
2. Collect data for 60 seconds
3. Verify `.parquet` files created
4. Test file reading via web interface

### Test 6.2: JSON Format
1. Set file format to `json`
2. Collect data for 60 seconds
3. Verify `.json` files created
4. Test file content is valid JSON

### Test 6.3: CSV Format
1. Set file format to `csv`
2. Collect data for 60 seconds
3. Verify `.csv` files created
4. Test file content has proper CSV structure

### Test 6.4: File Consolidation
1. Wait for daily consolidation (midnight UTC)
2. Verify consolidated files created
3. Check original files moved to `processed/` directory

---

## 7. S3 Integration Tests

### Test 7.1: S3 Configuration
1. Configure S3 settings in plugin config:
   ```json
   {
     "enabled": true,
     "bucket": "test-bucket",
     "region": "us-east-1",
     "keyPrefix": "marine-data/",
     "timing": "consolidation"
   }
   ```

### Test 7.2: S3 Connection Test
1. Navigate to **Cloud Status** tab
2. Click **Test S3 Connection**
3. Verify connection success or proper error message

### Test 7.3: S3 Upload Verification
1. Wait for file consolidation
2. Check S3 bucket for uploaded files
3. Verify correct key structure:
   ```
   marine-data/vessels/self/navigation/position/signalk_data_20250716_consolidated.parquet
   ```

---

## 8. API Endpoint Tests

### Test 8.1: Available Data Paths
```bash
curl http://localhost:3000/plugins/signalk-parquet/api/paths
```
**Expected**: List of available data paths with file counts

### Test 8.2: Path Configuration API
```bash
# Get path configurations
curl http://localhost:3000/plugins/signalk-parquet/api/config/paths

# Add path configuration
curl -X POST http://localhost:3000/plugins/signalk-parquet/api/config/paths \
  -H "Content-Type: application/json" \
  -d '{"path": "navigation.position", "enabled": true}'
```

### Test 8.3: Data Query API
```bash
curl -X POST http://localhost:3000/plugins/signalk-parquet/api/query \
  -H "Content-Type: application/json" \
  -d '{"query": "SELECT * FROM \"/path/to/data/*.parquet\" LIMIT 10"}'
```

### Test 8.4: Health Check
```bash
curl http://localhost:3000/plugins/signalk-parquet/api/health
```
**Expected**: `{"success": true, "status": "healthy", "duckdb": "available"}`

---

## 9. Performance Tests

### Test 9.1: High Volume Data
1. Configure multiple high-frequency paths
2. Set buffer size to 5000
3. Monitor system resource usage
4. Verify no memory leaks or performance degradation

### Test 9.2: Large File Handling
1. Let data collection run for 24 hours
2. Verify file consolidation handles large datasets
3. Check query performance on large files

### Test 9.3: Concurrent Access
1. Open multiple web interface tabs
2. Perform queries simultaneously
3. Verify no conflicts or errors

---

## 10. Error Handling Tests

### Test 10.1: Invalid Configuration
1. Configure invalid SignalK path
2. Verify graceful error handling
3. Check error messages in logs

### Test 10.2: Disk Space Issues
1. Fill disk space (test environment only)
2. Verify plugin handles disk full gracefully
3. Check error reporting

### Test 10.3: Network Issues
1. Disconnect network during S3 upload
2. Verify retry mechanism works
3. Check error logging

### Test 10.4: DuckDB Errors
1. Submit invalid SQL query
2. Verify proper error response
3. Check error doesn't crash plugin

---

## Automated Testing

### Test Script Example
```bash
#!/bin/bash
# Basic automated test script

echo "Testing SignalK Parquet Plugin..."

# Test 1: Plugin health
health_response=$(curl -s http://localhost:3000/plugins/signalk-parquet/api/health)
if echo "$health_response" | grep -q "healthy"; then
    echo "✅ Plugin health check passed"
else
    echo "❌ Plugin health check failed"
    exit 1
fi

# Test 2: Path configuration
config_response=$(curl -s http://localhost:3000/plugins/signalk-parquet/api/config/paths)
if echo "$config_response" | grep -q "success"; then
    echo "✅ Path configuration API working"
else
    echo "❌ Path configuration API failed"
    exit 1
fi

# Test 3: Data collection
sleep 30
data_response=$(curl -s http://localhost:3000/plugins/signalk-parquet/api/paths)
if echo "$data_response" | grep -q "paths"; then
    echo "✅ Data collection working"
else
    echo "❌ Data collection failed"
    exit 1
fi

echo "All tests passed!"
```

---

## Test Data Setup

### Sample Path Configurations
```json
{
  "paths": [
    {
      "path": "navigation.position",
      "enabled": true,
      "context": "vessels.self"
    },
    {
      "path": "environment.wind.angleApparent",
      "enabled": false,
      "regimen": "captureWeather",
      "source": "mqtt-weatherflow-udp",
      "context": "vessels.self"
    },
    {
      "path": "commands.captureWeather",
      "enabled": true,
      "context": "vessels.self"
    }
  ]
}
```

### Sample Test Queries
```sql
-- Basic data query
SELECT * FROM '/path/to/navigation/position/*.parquet' 
ORDER BY received_timestamp DESC LIMIT 10;

-- Aggregated data
SELECT 
  DATE_TRUNC('hour', received_timestamp::timestamp) as hour,
  AVG(value::double) as avg_value,
  COUNT(*) as record_count
FROM '/path/to/data/*.parquet'
GROUP BY hour
ORDER BY hour;

-- Source filtering
SELECT * FROM '/path/to/environment/wind/*.parquet' 
WHERE source_label = 'mqtt-weatherflow-udp'
ORDER BY received_timestamp DESC LIMIT 100;
```

---

## Test Environment Setup

### Development Environment
```bash
# Install dependencies
npm install

# Start in development mode
npm run dev

# Watch for changes
npm run watch
```

### Production Environment
```bash
# Build for production
npm run build

# Install in SignalK
cp -r . ~/.signalk/node_modules/signalk-parquet/

# Restart SignalK
sudo systemctl restart signalk
```

---

## Troubleshooting Test Issues

### Common Issues and Solutions

1. **Plugin Won't Load**
   - Check Node.js version (>=16.0.0)
   - Verify TypeScript compilation
   - Check SignalK logs for errors

2. **Web Interface 404**
   - Verify plugin is enabled
   - Check SignalK restart
   - Verify plugin registration

3. **No Data Collection**
   - Check path configuration via web interface
   - Verify SignalK data sources
   - Check plugin permissions

4. **DuckDB Query Errors**
   - Verify file paths in queries
   - Check file permissions
   - Ensure DuckDB dependency installed

5. **S3 Upload Failures**
   - Verify AWS credentials
   - Check S3 bucket permissions
   - Test network connectivity

---

## Test Results Template

### Test Execution Record
```
Date: ___________
Tester: ___________
Plugin Version: 0.5.0-alpha.2
SignalK Version: ___________
Node.js Version: ___________

Installation Tests:
[ ] GitHub Installation
[ ] TypeScript Compilation
[ ] Plugin Loading

Configuration Tests:
[ ] Basic Configuration
[ ] Configuration Persistence
[ ] Path Configuration Exclusion

Web Interface Tests:
[ ] Web App Access
[ ] Path Configuration Tab
[ ] Path Management
[ ] Path Configuration Persistence

Data Collection Tests:
[ ] Basic Data Collection
[ ] Data File Verification
[ ] Multi-Path Collection

Regimen Control Tests:
[ ] Regimen Configuration
[ ] Regimen Activation
[ ] Regimen Deactivation

File Output Tests:
[ ] Parquet Format
[ ] JSON Format
[ ] CSV Format
[ ] File Consolidation

S3 Integration Tests:
[ ] S3 Configuration
[ ] S3 Connection Test
[ ] S3 Upload Verification

API Endpoint Tests:
[ ] Available Data Paths
[ ] Path Configuration API
[ ] Data Query API
[ ] Health Check

Performance Tests:
[ ] High Volume Data
[ ] Large File Handling
[ ] Concurrent Access

Error Handling Tests:
[ ] Invalid Configuration
[ ] Disk Space Issues
[ ] Network Issues
[ ] DuckDB Errors

Overall Result: PASS / FAIL
Notes: ___________
```

---

## Contact

For testing questions or issues:
- **Plugin Issues**: Report via GitHub issues
- **SignalK Integration**: Check SignalK documentation
- **TypeScript Issues**: Consult TypeScript documentation
- **DuckDB Issues**: Check DuckDB documentation