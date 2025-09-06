# Claude API Implementation Comparison

## Overview

The SignalK Parquet plugin implements two distinct approaches for Claude AI analysis:

1. **REST API Access (Sampling Mode)** - Traditional single-shot analysis
2. **Database Access Mode** - Interactive multi-turn conversation with tools

---

## REST API Access (Sampling Mode)

**Implementation**: `analyzeData()` method (lines 128-167)

### Characteristics:
- **Single API Call**: Makes one call to Claude API with pre-loaded data
- **Data Pre-processing**: Calls `prepareDataForAnalysis()` to load and sample data via REST API
- **Static Prompt**: Uses `buildAnalysisPrompt()` to create a single prompt with data embedded
- **Simple Message Structure**: Single user message with all data included in prompt
- **No Interactive Tools**: Claude receives static data, no ability to make additional queries
- **One-shot Analysis**: Claude analyzes provided data sample and responds once

### Process Flow:
1. Load data via REST API (`loadDataFromPath()`)
2. Generate statistical summary (`generateDataSummary()`)
3. Sample data for token efficiency
4. Build comprehensive prompt with data embedded
5. Single Claude API call
6. Parse response and return results

### Limitations:
- Limited to pre-sampled data subset
- No ability to explore additional data
- Fixed analysis scope
- Cannot verify or cross-reference data

---

## Database Access Mode

**Implementation**: `analyzeWithDatabaseAccess()` method (lines 873-1400+)

### Characteristics:
- **Multi-turn Conversation**: Uses conversation loop with multiple API calls
- **Interactive Tools**: Claude has access to multiple tools:
  - `query_maritime_database` - Execute SQL queries
  - `get_current_signalk_data` - Real-time data access
  - `find_regimen_episodes` - Operational period detection
  - `generate_wind_analysis` - Specialized wind analysis prompts
- **System Context**: Comprehensive system prompt with:
  - Vessel context and specifications
  - Database schema information
  - Time range guidance
  - Data integrity rules
- **Tool Processing**: Processes tool calls via `processToolCall()` method
- **Query Limits**: Maximum 5 queries to prevent excessive API calls
- **Dynamic Analysis**: Claude can explore data, make queries, and build analysis iteratively

### Process Flow:
1. Load vessel context and database schema
2. Build comprehensive system context with rules and constraints
3. Initialize conversation with user prompt
4. **Conversation Loop**:
   - Claude analyzes request and decides what data to query
   - Makes tool calls (SQL queries, real-time data, etc.)
   - Processes tool results
   - Continues analysis or asks follow-up questions
   - Repeats until analysis complete or query limit reached
5. Compile final analysis from conversation history

### Advanced Features:
- **Conversation State**: Maintains full conversation history
- **Schema Access**: Dynamic database schema via `getEnhancedSchemaForClaude()`
- **Real-time Integration**: Can access current vessel status
- **Regimen Detection**: Understands operational contexts
- **Time Range Enforcement**: Strict time-based query constraints
- **Data Validation**: Multi-layered integrity checking

---

## Key Differences Summary

| Aspect | REST API Mode | Database Access Mode |
|--------|---------------|---------------------|
| **API Calls** | Single call | Multiple calls (max 5) |
| **Data Access** | Pre-sampled subset | Full database query access |
| **Interactivity** | Static analysis | Dynamic exploration |
| **Tools Available** | None | 4+ specialized tools |
| **Performance** | Faster (single call) | Slower (multiple exchanges) |
| **Token Usage** | Lower, predictable | Higher, variable |
| **Analysis Depth** | Limited to sample | Comprehensive, iterative |
| **Data Verification** | None | Can cross-reference and validate |
| **Follow-up Queries** | Not possible | Fully supported |
| **Real-time Data** | Not available | Integrated |
| **Conversation Memory** | None | Full history maintained |

---

## When to Use Each Mode

### REST API Mode (Sampling):
- Quick overviews and summaries
- Performance-critical applications
- Token budget constraints
- Simple data analysis tasks
- When data scope is well-defined

### Database Access Mode:
- Complex analytical tasks
- Investigative data exploration
- Real-time operational monitoring
- Multi-path correlation analysis
- When data integrity is critical
- Anomaly detection and pattern recognition

---

## Technical Implementation Notes

### REST API Mode Entry Point:
```typescript
if (request.useDatabaseAccess) {
  return await this.analyzeWithDatabaseAccess(request);
}
// Falls through to REST API sampling mode
const data = await this.prepareDataForAnalysis(request);
```

### Database Access System Context:
- Vessel specifications and operational context
- Complete database schema with available paths
- Time range constraints and SQL templates
- Data integrity validation rules
- Tool descriptions and usage guidelines

### Tool Processing Pipeline:
1. Parse tool calls from Claude response
2. Execute queries via `processToolCall()`
3. Format results for Claude consumption
4. Add tool results to conversation
5. Continue conversation loop

---

*This comparison reflects the current implementation as of the SignalK Parquet plugin codebase.*