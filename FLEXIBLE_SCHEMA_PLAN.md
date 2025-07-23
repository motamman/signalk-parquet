# Flexible Parquet Schema Plan for SignalK Data Types

## Problem Statement
SignalK `value` fields can contain multiple data types:
- **Numbers**: Sensor readings (temperature: 23.5, rpm: 1500)
- **Strings**: Status values ("running", "stopped", navigation.state: "anchored")
- **Booleans**: Binary states (engine.running: true/false)
- **Objects**: Complex data (position: {latitude: 45.123, longitude: -123.456})

Current implementation forces all values to UTF8 strings, losing type information and storage efficiency.

## Goals
1. **Preserve native data types** in Parquet files for better compression and query performance
2. **Handle mixed types gracefully** when the same SignalK path has different value types over time
3. **Maintain backward compatibility** with existing consolidated files
4. **Support schema evolution** during file consolidation

## Implementation Plan

### Phase 1: Data Analysis
- [ ] **Analyze SignalK data samples** to understand value field type distribution
  - **ACTION**: Run data analysis script on existing output files in `/data/` directory
  - **SPECIFIC TASKS**:
    - Create script to scan all `.json` and `.parquet` files in output directory
    - Group by SignalK path and analyze value types: `typeof value` distribution
    - Output report showing: path → [string%, number%, boolean%, object%]
    - Identify top 20 most frequent paths and their type consistency
    - Document complex object structures (like position coordinates, engine data)
  - **FILES TO EXAMINE**: Look in current plugin output directory structure
  - **EXPECTED OUTPUT**: `data_type_analysis_report.json` with type distribution by path

### Phase 2: Schema Strategy Design
- [ ] **Design flexible schema approach** for handling mixed value types
  - **ACTION**: Based on Phase 1 analysis, choose optimal schema strategy
  - **SPECIFIC DECISIONS NEEDED**:
    - Option A: Union types (if supported by @dsnp/parquetjs) - TEST FIRST
    - Option B: Separate typed columns (value_string, value_number, value_boolean, value_json)
    - Option C: Dynamic schema per file batch with fallback to string for mixed batches
  - **FILES TO MODIFY**: `src/parquet-writer.ts` schema definition sections
  - **EVALUATION CRITERIA**: File size impact, query performance, implementation complexity
  - **EXPECTED OUTPUT**: Decision document with chosen approach + prototype schema definition

### Phase 3: Core Implementation
- [ ] **Implement intelligent value type detection**
  - **FILES TO MODIFY**: `src/parquet-writer.ts:217-264` (existing `createParquetSchema` function)
  - **SPECIFIC CHANGES**:
    - Enhance `createParquetSchema()` to handle `value` field type detection separately
    - Add logic for complex object detection and JSON serialization
    - Handle null/undefined values appropriately per chosen schema strategy
  - **TEST**: Create unit tests for different value type combinations

- [ ] **Replace hardcoded UTF8 schema** with smart detection
  - **FILES TO MODIFY**: `src/parquet-writer.ts:110-140` (hardcoded schema definition)
  - **SPECIFIC CHANGES**:
    - Replace fixed `schemaFields` object with call to `this.createParquetSchema(records)`
    - Remove hardcoded `value: { type: 'UTF8', optional: true }` line
    - Keep SignalK metadata fields (context, path, source, timestamps) as UTF8
    - Update data preparation loop (lines 153-163) to handle typed values properly
  - **BACKWARDS COMPATIBILITY**: Ensure existing parquet files can still be read

### Phase 4: Schema Evolution Handling
- [ ] **Handle schema conflicts during consolidation**
  - **FILES TO MODIFY**: `src/parquet-writer.ts:287-339` (`mergeFiles` function)
  - **SPECIFIC CHANGES**:
    - Add schema compatibility checking before merging files
    - Implement type promotion logic (number → string when schemas conflict)
    - Add fallback to UTF8 for incompatible schemas during consolidation
  - **CONSOLIDATION IMPACT**: Update `consolidateDaily` to handle mixed schemas gracefully
  - **ERROR HANDLING**: Log schema conflicts and chosen resolution strategy

### Phase 5: Testing & Validation
- [ ] **Test mixed type scenarios**
  - **CREATE TEST DATA**: Generate sample SignalK data with mixed types for same paths
  - **TEST CASES**:
    - Same path with number → string → boolean values over time
    - Complex objects (position coordinates) mixed with simple values
    - Null/undefined values in typed columns
  - **VALIDATION**: Compare file sizes before/after typed implementation
  - **PERFORMANCE**: Benchmark query speed on numeric vs string value columns

- [ ] **Validate DuckDB compatibility**
  - **QUERY TESTING**: Run existing DuckDB queries against new typed Parquet files
  - **SPECIFIC TESTS**:
    - `SELECT AVG(value) FROM parquet_file WHERE path = 'propulsion.main.temperature'`
    - `SELECT value FROM parquet_file WHERE path = 'navigation.state'` (string values)
    - JSON extraction from complex object values
  - **REGRESSION TESTING**: Ensure all existing queries still work
  - **DOCUMENTATION**: Update query examples for typed columns

## Technical Considerations

### Schema Design Options

**Option A: Separate Typed Columns**
```typescript
const schemaFields = {
  // ... existing fields
  value_string: { type: 'UTF8', optional: true },
  value_number: { type: 'DOUBLE', optional: true },
  value_boolean: { type: 'BOOLEAN', optional: true },
  value_json: { type: 'UTF8', optional: true }, // for objects
  value_type: { type: 'UTF8', optional: false }, // 'string'|'number'|'boolean'|'object'
};
```

**Option B: Dynamic Schema Per Batch**
```typescript
// Analyze value types in current batch
// Create schema based on predominant type
// Fall back to UTF8 if mixed types detected
```

### Type Detection Logic
```typescript
function detectValueType(values: any[]): ParquetFieldType {
  const types = new Set(values.map(v => typeof v));
  
  if (types.size === 1) {
    // Homogeneous type
    const type = types.values().next().value;
    if (type === 'number') return allIntegers(values) ? 'INT64' : 'DOUBLE';
    if (type === 'boolean') return 'BOOLEAN';
    if (type === 'object' && values[0] !== null) return 'UTF8'; // JSON
  }
  
  // Mixed types or strings - fallback to UTF8
  return 'UTF8';
}
```

### Migration Strategy
- **Backward Compatibility**: Continue reading existing UTF8-only files
- **Forward Compatibility**: New files use flexible schema
- **Consolidation**: Handle mixed schemas during daily consolidation

## Success Metrics
- [ ] Reduced file sizes for numeric-heavy data (target: 20-40% reduction)
- [ ] Faster numeric aggregation queries in DuckDB
- [ ] Proper type preservation for boolean flags
- [ ] Graceful handling of schema evolution without data loss

## Risks & Mitigation
- **Schema incompatibility**: Implement robust fallback to UTF8
- **Parquet library limitations**: Test @dsnp/parquetjs type support thoroughly
- **Query compatibility**: Extensive DuckDB integration testing
- **Data corruption**: Comprehensive validation before production deployment

---

## WHEN RETURNING TO THIS WORK

### 🚀 Quick Start Checklist
1. **READ THIS PLAN FIRST** - Understand the current problem and approach
2. **CHECK CURRENT STATE**: 
   - Is plugin still using hardcoded UTF8 schema? (Check `src/parquet-writer.ts:110-140`)
   - Are there existing output files to analyze? (Look in plugin output directory)
3. **START WITH PHASE 1**: Create data analysis script to understand current value type distribution
4. **VALIDATE PARQUET LIBRARY**: Test what data types @dsnp/parquetjs actually supports before proceeding

### 📁 Key Files to Examine When Starting
- `src/parquet-writer.ts:110-140` - Current hardcoded schema
- `src/parquet-writer.ts:217-264` - Existing `createParquetSchema` function (already implemented!)
- `src/parquet-writer.ts:287-339` - File merging logic that needs schema evolution handling
- Plugin output directory - Existing data files for analysis

### 🎯 Priority Order
1. **HIGH**: Data analysis (understand what we're working with)
2. **HIGH**: Schema strategy decision (based on analysis results)
3. **MEDIUM**: Core implementation (use existing smart schema function)
4. **MEDIUM**: Schema evolution handling (for consolidation)
5. **LOW**: Testing and validation

### ⚠️ Critical Notes
- **DON'T BREAK EXISTING DATA**: Ensure backward compatibility with current UTF8 files
- **TEST PARQUET LIBRARY LIMITS**: Verify @dsnp/parquetjs supports all needed types before implementing
- **CONSOLIDATION IMPACT**: Schema changes will affect daily file consolidation - plan carefully