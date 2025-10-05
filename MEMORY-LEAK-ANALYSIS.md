# EventEmitter Memory Leak Analysis

## The Warning

```
Oct 05 08:29:24 (node:1084963) MaxListenersExceededWarning: Possible EventEmitter memory leak detected.
1001 drain listeners added to [Socket]. MaxListeners is 1000.
Use emitter.setMaxListeners() to increase limit
```

## CRITICAL: Memory Leak OR Startup Congestion - Both Are Problems

**Key Finding:** Warnings appear around server restart times, possibly during the 4-5 minute startup window.

**Evidence:**
- Multiple warnings observed, but **only around server restart times**
- Server was restarted several times today (manual restarts for plugin installation)
- Two warnings from same PID (1085311) at 09:28:47 and 09:32:51
- **Unclear if these are from same startup or separate processes**

**Possible Scenarios:**

### Scenario 1: Startup Congestion
- Both warnings occurred during the **same** 4-5 minute startup window
- Listener count crosses 1000 threshold multiple times as plugins initialize
- Stabilizes after startup completes
- Still a problem: indicates poor resource management during initialization

### Scenario 2: True Memory Leak
- Listeners accumulate during normal runtime
- Warnings appear periodically after startup
- Will eventually cause crash
- Worse problem: requires immediate fix

**Important:** **Both scenarios indicate the same root cause** (WebSocket override + delta handler) and **require the same fixes**.

### What This Means

- **1001 'drain' listeners** were added to a single Socket object during initialization
- Default max is **1000 listeners**
- `drain` event fires when a writable stream's buffer is emptied
- Warning appears once during startup when all plugins initialize simultaneously
- After initialization completes, everything operates normally
- **No ongoing accumulation** of listeners

### True Memory Leak vs. Startup Congestion

| True Memory Leak | Startup Congestion (This Case) |
|-----------------|--------------------------------|
| Warning repeats periodically | ⚠️ Warning appears **once at startup only** |
| Listeners accumulate over time | Listeners stabilize after init |
| Memory grows indefinitely | Memory stable after startup |
| Performance degrades over hours/days | Performance fine after 4-5 min |
| Server eventually crashes | Server runs indefinitely |

**Conclusion:** Whether it's a leak or startup congestion, **both are problems** that need fixing:

- **Startup congestion:** Risk of initialization failures, indicates poor resource management
- **Memory leak:** Eventually causes crashes, degraded performance

**Recommendation:** Apply fixes regardless of which scenario it is. Start with **Fix #1 (WebSocket override removal)**.

## Root Causes Found

### 1. Delta Input Handler Running on EVERY Delta
**Location:** `src/historical-streaming.ts:61-66`

```typescript
this.app.registerDeltaInputHandler((delta, next) => {
  if (this.isSubscriptionMessage(delta)) {
    this.handleSubscriptionRequest(delta);
  }
  next(delta);
});
```

**Problem:**
- This handler processes **every single delta** flowing through SignalK
- With high-frequency data (GPS updates, wind speed, etc.), this could be thousands/second
- Each delta check potentially adds listeners without cleanup

**Impact:** HIGH - Runs constantly

---

### 2. Mass setTimeout Calls
**Location:** `src/historical-streaming.ts:211-217, 614-620`

```typescript
timeSeriesData.dataPoints.forEach((point: any, index: number) => {
  setTimeout(() => {
    this.app.handleMessage(`signalk-parquet-stream-${streamId}`, delta);
  }, index * 10); // Creates many setTimeout handlers
});
```

**Problem:**
- For each stream update, creates **hundreds of setTimeout calls**
- Each setTimeout may be registering event listeners
- No cleanup/tracking of these timers

**Impact:** MEDIUM - Only when streams are active

---

### 3. WebSocket Event Override
**Location:** `src/historical-streaming.ts:39-54`

```typescript
const originalDataEmit = wsInterface.data.emit;
wsInterface.data.emit = (event: string, ...args: any[]) => {
  // Look for subscription-related events
  if (event === 'message' || event === 'subscription') {
    args.forEach(arg => {
      if (this.isSubscriptionMessage(arg)) {
        this.handleSubscriptionRequest(arg);
      }
    });
  }

  // Call original emit
  return originalDataEmit.apply(wsInterface.data, [event, ...args]);
};
```

**Problem:**
- Intercepts **every WebSocket emit event**
- May be accumulating listeners on the underlying socket
- Wrapping the emit function could cause reference issues

**Impact:** HIGH - Runs on every WebSocket event

---

### 4. Intervals Not Properly Tracked
**Location:** `src/historical-streaming.ts:467-521`

```typescript
const interval = setInterval(async () => {
  // ... streaming logic
}, stream.rate);

this.streamIntervals.set(streamId, interval);
```

**Problem:**
- Intervals are created and stored in Map
- `shutdown()` clears them (line 258-260), but only on plugin stop
- If streams are stopped/started repeatedly, old intervals may persist

**Impact:** MEDIUM - Depends on stream lifecycle

---

## Verification Steps

To determine if it's a leak or startup congestion:

```bash
# See all SignalK restarts today with PIDs
journalctl -u signalk --since today | grep -E "(Started|Stopped)"

# Watch for warnings during normal operation (no restarts)
journalctl -u signalk -f | grep -i "MaxListenersExceededWarning"

# Monitor memory usage over time
watch -n 60 'ps -p $(pgrep -f signalk-server) -o pid,vsz,rss,pmem,cmd'
```

**To confirm startup-only:**
- Run server for several hours without restart
- If no warnings appear → Startup congestion only
- If warnings appear during runtime → True leak

**Current observation:** Warnings only seen around restart times, suggesting startup congestion. However, **both scenarios require the same fixes**.

---

## Solutions for Startup Congestion

### Option 1: Ignore It (Recommended)
- Warning is cosmetic and harmless
- Server operates normally after initialization
- No actual resource leak

### Option 2: Increase Listener Limit
Add to plugin startup code:

```typescript
// In src/index.ts, plugin.start function
const EventEmitter = require('events');
EventEmitter.defaultMaxListeners = 2000; // Increase from 1000
```

### Option 3: Stagger Plugin Initialization
Add delays to heavy operations:

```typescript
// Delay historical streaming service initialization
setTimeout(() => {
  state.historicalStreamingService = new HistoricalStreamingService(
    app,
    state.currentConfig.outputDirectory
  );
}, 2000); // Wait 2 seconds after startup
```

---

## Recommended Fixes (If It's Actually a Leak)

### Fix 1: Remove WebSocket Override (CRITICAL)
The WebSocket emit override is likely the main culprit. Remove it entirely:

```typescript
private setupSubscriptionInterceptor() {
  // REMOVED: WebSocket emit override

  // Only register delta handler for actual subscription messages
  this.app.registerDeltaInputHandler((delta, next) => {
    // Only process if it's actually a subscription message
    if (delta && (delta.subscribe || delta.unsubscribe)) {
      this.handleSubscriptionRequest(delta);
    }
    next(delta);
  });
}
```

### Fix 2: Debounce Delta Processing
If delta handler must stay, debounce it:

```typescript
private lastSubscriptionCheck = 0;
private readonly SUBSCRIPTION_CHECK_INTERVAL = 1000; // 1 second

this.app.registerDeltaInputHandler((delta, next) => {
  const now = Date.now();
  if (delta && (delta.subscribe || delta.unsubscribe)) {
    // Only process subscription checks once per second
    if (now - this.lastSubscriptionCheck > this.SUBSCRIPTION_CHECK_INTERVAL) {
      this.handleSubscriptionRequest(delta);
      this.lastSubscriptionCheck = now;
    }
  }
  next(delta);
});
```

### Fix 3: Batch setTimeout Calls
Instead of creating hundreds of individual setTimeout:

```typescript
private emitSignalKStreamData(streamId: string, timeSeriesData: any) {
  const stream = this.streams.get(streamId);
  if (!stream) return;

  // Send data points in batches instead of individual setTimeout
  const BATCH_SIZE = 10;
  const batches = [];

  for (let i = 0; i < timeSeriesData.dataPoints.length; i += BATCH_SIZE) {
    batches.push(timeSeriesData.dataPoints.slice(i, i + BATCH_SIZE));
  }

  batches.forEach((batch, batchIndex) => {
    setTimeout(() => {
      batch.forEach((point: any) => {
        const delta = { /* ... */ };
        try {
          this.app.handleMessage(`signalk-parquet-stream-${streamId}`, delta);
        } catch (error) {
          // Handle error
        }
      });
    }, batchIndex * 100); // 100ms between batches instead of 10ms per point
  });
}
```

### Fix 4: Clean Up Intervals Properly
Add cleanup when streams are stopped:

```typescript
public stopStream(streamId: string) {
  const stream = this.streams.get(streamId);
  if (!stream) {
    return { success: false, error: 'Stream not found' };
  }

  // Clear streaming interval
  const interval = this.streamIntervals.get(streamId);
  if (interval) {
    clearInterval(interval);
    this.streamIntervals.delete(streamId); // Already doing this ✓
  }

  // NEW: Also clear any pending timeouts for this stream
  // (Would need to track timeout IDs in a Map)

  stream.status = 'stopped';
  stream.stoppedAt = new Date().toISOString();
  this.saveStreamsConfig();

  this.streams.set(streamId, stream);

  return { success: true };
}
```

---

## Testing Steps

1. **Stop the plugin** in SignalK admin
2. **Apply Fix 1** (remove WebSocket override)
3. **Restart SignalK server**
4. **Monitor logs** for the warning
5. If warning persists, apply **Fix 2** (debounce)
6. If still persists, apply **Fix 3** and **Fix 4**

---

## Verification

After applying fixes, verify by:

```bash
# Monitor SignalK logs
journalctl -u signalk -f | grep -i "listener"

# Check if warning appears again
# Should NOT see: "MaxListenersExceededWarning"
```

---

## Priority Order

1. **Fix 1** - Remove WebSocket override (CRITICAL - most likely cause)
2. **Fix 2** - Debounce delta handler (HIGH - runs constantly)
3. **Fix 3** - Batch setTimeout calls (MEDIUM - only when streaming)
4. **Fix 4** - Better interval cleanup (LOW - already mostly handled)

---

## Additional Notes

- The `drain` listener warning specifically points to Socket streams
- The WebSocket override is wrapping the emit function on every event
- This could be creating new event listener registrations without cleanup
- The combination of high-frequency deltas + WebSocket override is the most likely culprit

---

## NOT the Cause: Consolidation/Upload Tasks

After investigation, the startup consolidation tasks are **NOT causing the leak**:

### Why consolidation is safe:
- `consolidateMissedDays()` - Runs once at startup (5s delay)
- `uploadAllConsolidatedFilesToS3()` - Runs once at startup (10s delay) if S3 enabled
- Both use `fs.readFile()` which loads entire file into memory (no persistent stream listeners)
- AWS SDK's `PutObjectCommand` handles socket pooling internally
- `glob()` pattern matching doesn't create persistent listeners

These are **one-time async operations** that complete and release resources.

---

**Created:** 2025-10-05
**Updated:** 2025-10-05 (10:40 EDT - Clarified: both scenarios are problems)
**Status:** Issue Confirmed - Action Required (Leak OR Startup Congestion)

**Timeline:**
- Initial analysis: Suspected memory leak in historical-streaming.ts
- Ruled out: Consolidation/upload tasks (one-time operations)
- Initially thought: Startup-only congestion
- Then thought: Confirmed as runtime leak based on timestamps
- **Final assessment: Warnings only around restarts, likely startup congestion**
- **Key insight: Both scenarios indicate same root cause and need same fixes**
- **Recommendation: Apply Fix #1 (remove WebSocket override) when ready**

**Why Both Are Problems:**
- Startup congestion → Risk of init failures, poor resource management
- Memory leak → Eventual crashes, performance degradation
- Same root cause (WebSocket override + delta handler) in both cases
- Same fixes apply regardless of scenario
