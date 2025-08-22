# Historical Data Streaming Implementation Instructions

## Current Status
We are implementing historical data streaming using SignalK's existing WebSocket infrastructure instead of creating a separate WebSocket server (which was causing admin UI crashes).

## What We've Done So Far

### 1. Problem Identification
- **Issue**: Separate WebSocket server was conflicting with SignalK's admin UI, causing constant reloading
- **Root Cause**: Creating our own WebSocket server competed with SignalK's core WebSocket functionality
- **Solution**: Use SignalK's existing WebSocket infrastructure via `registerDeltaInputHandler` and `handleMessage`

### 2. Architecture Approach
**OLD (Problematic)**: 
- Created separate WebSocket server on same HTTP server
- Used custom WebSocket protocol
- Conflicted with SignalK admin UI

**NEW (Current)**: 
- Hook into SignalK's existing `/signalk/v1/stream` WebSocket endpoint
- Intercept subscription messages using `registerDeltaInputHandler`
- Inject historical data using `handleMessage`
- No separate WebSocket server needed

### 3. Files Created/Modified

#### A. New File: `src/historical-streaming.ts`
**Purpose**: Main historical streaming service that hooks into SignalK's WebSocket infrastructure

**Key Components**:
- `HistoricalStreamingService` class
- `setupSubscriptionInterceptor()` - hooks into SignalK delta stream
- `isSubscriptionMessage()` - detects WebSocket subscription requests
- `isHistoricalDataPath()` - determines which paths trigger historical data
- `streamSampleHistoricalData()` - sends historical data as delta messages

**Current Implementation State**: 
- Basic structure in place
- Has TypeScript compilation errors that need fixing
- Uses sample data instead of real historical data (temporary)

#### B. Modified: `src/types.ts`
**Changes**:
- Added `historicalStreamingService?: any` to `PluginState` interface
- Avoids circular import issues

#### C. Modified: `src/index.ts`
**Changes**:
- Added import for `HistoricalStreamingService`
- Initialize service in plugin start: `state.historicalStreamingService = new HistoricalStreamingService(app)`
- Added cleanup in plugin stop: `state.historicalStreamingService.shutdown()`

#### D. Modified: `src/HistoryAPI.ts`
**Changes**:
- Added `export` to `class HistoryAPI` (was missing export)

## Current TypeScript Compilation Errors to Fix

### 1. Delta Message Type Issues
**Error**: Delta message format doesn't match SignalK's expected types
**Location**: `historical-streaming.ts` line 123
**Fix Needed**: 
```typescript
// Current (broken):
const delta = {
  context: 'vessels.self' as Context,
  updates: [{
    timestamp: data.timestamp.toISOString(), // String not allowed
    values: [{
      path: path as Path,
      value: data.value
    }]
  }]
};

// Should be:
const delta = {
  context: 'vessels.self' as Context,
  updates: [{
    timestamp: data.timestamp.toISOString() as Timestamp,
    values: [{
      path: path as Path,
      value: data.value
    }]
  }]
};
```

### 2. Import Missing Types
**Fix**: Add `Timestamp` to imports:
```typescript
import { ServerAPI, Context, Path, Timestamp } from '@signalk/server-api';
```

### 3. Unused Parameters
**Fix**: Remove or use `subscriptionId` parameter in `streamSampleHistoricalData`

## Next Steps to Complete Implementation

### Immediate (Fix Compilation):
1. Add `Timestamp` import to `historical-streaming.ts`
2. Cast timestamp string to `Timestamp` type
3. Fix unused parameter warnings
4. Test compilation with `npm run build`

### Short Term (Basic Functionality):
1. **Test Basic Subscription Interception**:
   - Install plugin and monitor logs
   - Use browser dev tools to connect to `/signalk/v1/stream`
   - Send subscription message and see if interceptor catches it

2. **Improve Path Detection**:
   - Currently detects `navigation.*`, `environment.*`, and `history.*` paths
   - Add configuration for which paths trigger historical data
   - Consider special prefix like `history.navigation.position` for explicit historical requests

3. **Replace Sample Data with Real Historical Data**:
   - Connect to actual HistoryAPI.getValues() method
   - Handle the complex method signature (7 parameters)
   - Or create simplified wrapper method

### Medium Term (Full Implementation):
1. **WebSocket Subscription Protocol**:
   - Study SignalK's actual subscription message format
   - Ensure we're properly detecting real subscription requests
   - Handle subscription/unsubscription lifecycle

2. **Historical Data Integration**:
   - Replace sample data with real Parquet file queries
   - Support time range parameters from subscription messages
   - Handle aggregation methods (average, min, max, etc.)

3. **Performance & Streaming**:
   - Implement proper streaming for large datasets
   - Add rate limiting to prevent overwhelming clients
   - Support real-time updates after historical seed data

### Long Term (Advanced Features):
1. **Subscription Management**:
   - Track active historical subscriptions
   - Support subscription updates/modifications
   - Proper cleanup on client disconnect

2. **Advanced Query Support**:
   - Support complex time range queries
   - Multiple path subscriptions
   - Custom aggregation periods

## Testing Plan

### Phase 1: Basic Interception
- Install plugin with current (fixed) code
- Monitor SignalK logs for "Setting up historical data subscription interceptor"
- Use browser to connect to WebSocket and send test subscription

### Phase 2: Delta Injection
- Verify that `handleMessage` successfully injects data into SignalK stream
- Test that clients receive the injected historical data
- Confirm no conflicts with live SignalK data

### Phase 3: Integration
- Test with real historical data from Parquet files
- Verify performance with large datasets
- Test multiple concurrent subscriptions

## Key SignalK APIs Used

1. **`app.registerDeltaInputHandler(handler)`**:
   - Intercepts ALL incoming delta messages
   - Used to catch subscription requests from WebSocket clients

2. **`app.handleMessage(id, deltaMessage)`**:
   - Injects delta messages into SignalK's stream
   - Historical data appears as normal SignalK deltas to clients

3. **WebSocket Endpoint**: `/signalk/v1/stream`
   - Clients connect to this standard SignalK endpoint
   - No custom WebSocket server needed

## Files to Monitor for Issues
- SignalK server logs during startup
- Browser console when connecting to WebSocket
- Admin UI functionality (should not crash anymore)
- Network tab showing WebSocket message flow

## Success Criteria
1. ✅ No admin UI crashes (achieved by removing separate WebSocket server)
2. 🔄 TypeScript compilation succeeds
3. ⏳ Subscription messages are intercepted and logged
4. ⏳ Historical data is injected as delta messages
5. ⏳ Clients receive historical data through standard SignalK WebSocket
6. ⏳ No conflicts with live SignalK data streams

## Current Branch
All work is on the new clean branch (removed problematic streaming service code).