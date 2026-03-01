#!/bin/bash
# REST API Test Suite for SignalK Parquet Plugin
# Comprehensive testing of all REST API endpoints

set -e

# Load environment variables from .env file
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
if [[ -f "${SCRIPT_DIR}/.env" ]]; then
    source "${SCRIPT_DIR}/.env"
else
    echo "Warning: ${SCRIPT_DIR}/.env not found. Copy .env.example to .env and configure."
fi

# Configuration (with defaults if not set in .env)
BASE_URL="${BASE_URL:-http://localhost:3000}"
USERNAME="${SK_USERNAME:-}"
PASSWORD="${SK_PASSWORD:-}"
TEST_PATH="environment.wind.speedApparent"
TEST_LAT="40.646226666666664"
TEST_LON="-73.981275"

# Check required variables
if [[ -z "$USERNAME" || -z "$PASSWORD" ]]; then
    echo "Error: SK_USERNAME and SK_PASSWORD must be set in .env file"
    exit 1
fi

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Counters
PASSED=0
FAILED=0
SKIPPED=0

# Results array
declare -a RESULTS

# Helper functions
log_test() {
    echo -e "${BLUE}[TEST]${NC} $1"
}

log_pass() {
    echo -e "${GREEN}[PASS]${NC} $1"
    ((PASSED++))
    RESULTS+=("PASS: $1")
}

log_fail() {
    echo -e "${RED}[FAIL]${NC} $1"
    echo -e "${RED}       Response: $2${NC}"
    ((FAILED++))
    RESULTS+=("FAIL: $1 - $2")
}

log_skip() {
    echo -e "${YELLOW}[SKIP]${NC} $1"
    ((SKIPPED++))
    RESULTS+=("SKIP: $1")
}

log_section() {
    echo ""
    echo -e "${YELLOW}========================================${NC}"
    echo -e "${YELLOW} $1${NC}"
    echo -e "${YELLOW}========================================${NC}"
}

# Check if response is successful (HTTP 200 and valid JSON)
check_response() {
    local response="$1"
    local test_name="$2"
    local expected_field="$3"

    # Check for curl errors
    if [[ "$response" == *"curl:"* ]] || [[ -z "$response" ]]; then
        log_fail "$test_name" "Connection error or empty response"
        return 1
    fi

    # Check for error in JSON response
    if echo "$response" | jq -e '.error' > /dev/null 2>&1; then
        local error=$(echo "$response" | jq -r '.error')
        log_fail "$test_name" "Error: $error"
        return 1
    fi

    # Check for expected field if specified
    if [[ -n "$expected_field" ]]; then
        if echo "$response" | jq -e ".$expected_field" > /dev/null 2>&1; then
            log_pass "$test_name"
            return 0
        else
            log_fail "$test_name" "Missing expected field: $expected_field"
            return 1
        fi
    fi

    # Basic success check
    if echo "$response" | jq . > /dev/null 2>&1; then
        log_pass "$test_name"
        return 0
    else
        log_fail "$test_name" "Invalid JSON response"
        return 1
    fi
}

# Make authenticated request
auth_request() {
    local method="$1"
    local endpoint="$2"
    local data="$3"

    if [[ "$method" == "POST" ]]; then
        curl -s -X POST "${BASE_URL}${endpoint}" \
            -H "Authorization: Bearer $TOKEN" \
            -H "Content-Type: application/json" \
            -d "$data"
    else
        curl -s "${BASE_URL}${endpoint}" \
            -H "Authorization: Bearer $TOKEN"
    fi
}

# ============================================================
# AUTHENTICATION
# ============================================================
log_section "Authentication"

log_test "Obtaining bearer token..."
AUTH_RESPONSE=$(curl -s -X POST "${BASE_URL}/signalk/v1/auth/login" \
    -H "Content-Type: application/json" \
    -d "{\"username\": \"${USERNAME}\", \"password\": \"${PASSWORD}\"}")

TOKEN=$(echo "$AUTH_RESPONSE" | jq -r '.token // empty')

if [[ -z "$TOKEN" ]]; then
    echo -e "${RED}Failed to obtain bearer token!${NC}"
    echo "Response: $AUTH_RESPONSE"
    echo ""
    echo "Make sure SignalK server is running at $BASE_URL"
    echo "and credentials are correct (username: $USERNAME)"
    exit 1
fi

echo -e "${GREEN}Successfully obtained bearer token${NC}"
echo "Token: ${TOKEN:0:20}..."

# ============================================================
# A. HEALTH & STATUS ENDPOINTS
# ============================================================
log_section "A. Health & Status Endpoints"

# A1: Health check
log_test "A1: Health check"
RESPONSE=$(auth_request GET "/plugins/signalk-parquet/api/health")
check_response "$RESPONSE" "A1: Health check" "status"

# A2: Buffer stats
log_test "A2: Buffer stats"
RESPONSE=$(auth_request GET "/plugins/signalk-parquet/api/buffer/stats")
check_response "$RESPONSE" "A2: Buffer stats"

# A3: Buffer health
log_test "A3: Buffer health"
RESPONSE=$(auth_request GET "/plugins/signalk-parquet/api/buffer/health")
check_response "$RESPONSE" "A3: Buffer health"

# ============================================================
# B. DISCOVERY ENDPOINTS
# ============================================================
log_section "B. Discovery Endpoints"

# B1: List all paths
log_test "B1: List all paths"
RESPONSE=$(auth_request GET "/plugins/signalk-parquet/api/paths")
check_response "$RESPONSE" "B1: List all paths"

# B2: Get contexts (no time)
log_test "B2: Get contexts (no time)"
RESPONSE=$(auth_request GET "/signalk/v1/history/contexts")
check_response "$RESPONSE" "B2: Get contexts (no time)"

# B3: Get contexts (with time)
log_test "B3: Get contexts (with duration=1h)"
RESPONSE=$(auth_request GET "/signalk/v1/history/contexts?duration=1h")
check_response "$RESPONSE" "B3: Get contexts (with time)"

# B4: Get paths (no time)
log_test "B4: Get paths (no time)"
RESPONSE=$(auth_request GET "/signalk/v1/history/paths")
check_response "$RESPONSE" "B4: Get paths (no time)"

# B5: Get paths (with time)
log_test "B5: Get paths (with duration=24h)"
RESPONSE=$(auth_request GET "/signalk/v1/history/paths?duration=24h")
check_response "$RESPONSE" "B5: Get paths (with time)"

# B6: V2 contexts endpoint (NOTE: V2 API requires ISO 8601 duration format)
log_test "B6: V2 contexts endpoint"
RESPONSE=$(auth_request GET "/signalk/v2/api/history/contexts?duration=PT1H")
check_response "$RESPONSE" "B6: V2 contexts endpoint"

# B7: V2 paths endpoint
log_test "B7: V2 paths endpoint"
RESPONSE=$(auth_request GET "/signalk/v2/api/history/paths?duration=PT1H")
check_response "$RESPONSE" "B7: V2 paths endpoint"

# ============================================================
# C. TIME RANGE PATTERNS
# ============================================================
log_section "C. Time Range Patterns (5 Standard Patterns)"

# C1: Duration only
log_test "C1: Duration only (?duration=1h)"
RESPONSE=$(auth_request GET "/signalk/v1/history/values?duration=1h&paths=${TEST_PATH}")
check_response "$RESPONSE" "C1: Duration only" "range"

# C2: From + duration
log_test "C2: From + duration"
RESPONSE=$(auth_request GET "/signalk/v1/history/values?from=2025-02-28T00:00:00Z&duration=6h&paths=${TEST_PATH}")
check_response "$RESPONSE" "C2: From + duration" "range"

# C3: To + duration
log_test "C3: To + duration"
RESPONSE=$(auth_request GET "/signalk/v1/history/values?to=2025-03-01T12:00:00Z&duration=2h&paths=${TEST_PATH}")
check_response "$RESPONSE" "C3: To + duration" "range"

# C4: From only (to now)
log_test "C4: From only (to now)"
RESPONSE=$(auth_request GET "/signalk/v1/history/values?from=2025-03-01T00:00:00Z&paths=${TEST_PATH}")
check_response "$RESPONSE" "C4: From only" "range"

# C5: From + to
log_test "C5: From + to (explicit range)"
RESPONSE=$(auth_request GET "/signalk/v1/history/values?from=2025-02-28T00:00:00Z&to=2025-03-01T00:00:00Z&paths=${TEST_PATH}")
check_response "$RESPONSE" "C5: From + to" "range"

# ============================================================
# D. DURATION FORMATS
# ============================================================
log_section "D. Duration Formats"

# D1: ISO 8601
log_test "D1: ISO 8601 format (PT1H)"
RESPONSE=$(auth_request GET "/signalk/v1/history/values?duration=PT1H&paths=${TEST_PATH}")
check_response "$RESPONSE" "D1: ISO 8601 (PT1H)" "range"

# D2: ISO compound
log_test "D2: ISO compound format (PT1H30M)"
RESPONSE=$(auth_request GET "/signalk/v1/history/values?duration=PT1H30M&paths=${TEST_PATH}")
check_response "$RESPONSE" "D2: ISO compound (PT1H30M)" "range"

# D3: Integer seconds
log_test "D3: Integer seconds (3600)"
RESPONSE=$(auth_request GET "/signalk/v1/history/values?duration=3600&paths=${TEST_PATH}")
check_response "$RESPONSE" "D3: Integer seconds (3600)" "range"

# D4: Shorthand hours
log_test "D4: Shorthand hours (1h)"
RESPONSE=$(auth_request GET "/signalk/v1/history/values?duration=1h&paths=${TEST_PATH}")
check_response "$RESPONSE" "D4: Shorthand hours (1h)" "range"

# D5: Shorthand minutes
log_test "D5: Shorthand minutes (30m)"
RESPONSE=$(auth_request GET "/signalk/v1/history/values?duration=30m&paths=${TEST_PATH}")
check_response "$RESPONSE" "D5: Shorthand minutes (30m)" "range"

# D6: Shorthand days
log_test "D6: Shorthand days (2d)"
RESPONSE=$(auth_request GET "/signalk/v1/history/values?duration=2d&paths=${TEST_PATH}")
check_response "$RESPONSE" "D6: Shorthand days (2d)" "range"

# ============================================================
# E. AGGREGATION METHODS
# ============================================================
log_section "E. Aggregation Methods"

# E1: Average
log_test "E1: Average aggregation"
RESPONSE=$(auth_request GET "/signalk/v1/history/values?duration=1h&resolution=60&paths=${TEST_PATH}:average")
check_response "$RESPONSE" "E1: Average aggregation" "values"

# E2: Min
log_test "E2: Min aggregation"
RESPONSE=$(auth_request GET "/signalk/v1/history/values?duration=1h&resolution=60&paths=${TEST_PATH}:min")
check_response "$RESPONSE" "E2: Min aggregation" "values"

# E3: Max
log_test "E3: Max aggregation"
RESPONSE=$(auth_request GET "/signalk/v1/history/values?duration=1h&resolution=60&paths=${TEST_PATH}:max")
check_response "$RESPONSE" "E3: Max aggregation" "values"

# E4: First
log_test "E4: First aggregation"
RESPONSE=$(auth_request GET "/signalk/v1/history/values?duration=1h&resolution=60&paths=${TEST_PATH}:first")
check_response "$RESPONSE" "E4: First aggregation" "values"

# E5: Last
log_test "E5: Last aggregation"
RESPONSE=$(auth_request GET "/signalk/v1/history/values?duration=1h&resolution=60&paths=${TEST_PATH}:last")
check_response "$RESPONSE" "E5: Last aggregation" "values"

# E6: Mid
log_test "E6: Mid aggregation"
RESPONSE=$(auth_request GET "/signalk/v1/history/values?duration=1h&resolution=60&paths=${TEST_PATH}:mid")
check_response "$RESPONSE" "E6: Mid aggregation" "values"

# E7: SMA (Simple Moving Average)
log_test "E7: SMA (Simple Moving Average)"
RESPONSE=$(auth_request GET "/signalk/v1/history/values?duration=1h&paths=${TEST_PATH}:sma:5")
check_response "$RESPONSE" "E7: SMA (sma:5)" "values"

# E8: EMA (Exponential Moving Average)
log_test "E8: EMA (Exponential Moving Average)"
RESPONSE=$(auth_request GET "/signalk/v1/history/values?duration=1h&paths=${TEST_PATH}:ema:0.3")
check_response "$RESPONSE" "E8: EMA (ema:0.3)" "values"

# E9: Multiple aggregations
log_test "E9: Multiple aggregations (min, max, average)"
RESPONSE=$(auth_request GET "/signalk/v1/history/values?duration=1h&resolution=60&paths=${TEST_PATH}:min,${TEST_PATH}:max,${TEST_PATH}:average")
check_response "$RESPONSE" "E9: Multiple aggregations" "values"

# ============================================================
# F. EXTENSION PARAMETERS
# ============================================================
log_section "F. Extension Parameters"

# F1: Local timezone conversion
log_test "F1: Local timezone conversion"
RESPONSE=$(auth_request GET "/signalk/v1/history/values?duration=1h&paths=${TEST_PATH}&convertTimesToLocal=true")
check_response "$RESPONSE" "F1: Local timezone" "range"

# F2: Specific timezone
log_test "F2: Specific timezone (America/New_York)"
RESPONSE=$(auth_request GET "/signalk/v1/history/values?duration=1h&paths=${TEST_PATH}&convertTimesToLocal=true&timezone=America/New_York")
check_response "$RESPONSE" "F2: America/New_York timezone" "range"

# F3: Unit conversion
log_test "F3: Unit conversion"
RESPONSE=$(auth_request GET "/signalk/v1/history/values?duration=1h&paths=${TEST_PATH}&convertUnits=true")
check_response "$RESPONSE" "F3: Unit conversion" "range"

# F4: Include moving averages
log_test "F4: Include moving averages"
RESPONSE=$(auth_request GET "/signalk/v1/history/values?duration=1h&paths=${TEST_PATH}&includeMovingAverages=true")
check_response "$RESPONSE" "F4: Include moving averages" "range"

# F5: Auto-refresh
log_test "F5: Auto-refresh (pattern 1 only)"
RESPONSE=$(auth_request GET "/signalk/v1/history/values?duration=15m&paths=${TEST_PATH}&refresh=true")
check_response "$RESPONSE" "F5: Auto-refresh" "range"

# F6: Resolution in seconds
log_test "F6: Resolution in seconds (60)"
RESPONSE=$(auth_request GET "/signalk/v1/history/values?duration=1h&paths=${TEST_PATH}&resolution=60")
check_response "$RESPONSE" "F6: Resolution (60s)" "range"

# F7: Resolution expression
log_test "F7: Resolution expression (5m)"
RESPONSE=$(auth_request GET "/signalk/v1/history/values?duration=1h&paths=${TEST_PATH}&resolution=5m")
check_response "$RESPONSE" "F7: Resolution (5m)" "range"

# F8: Context selection
log_test "F8: Context selection (vessels.self)"
RESPONSE=$(auth_request GET "/signalk/v1/history/values?duration=1h&paths=${TEST_PATH}&context=vessels.self")
check_response "$RESPONSE" "F8: Context selection" "context"

# ============================================================
# G. SPATIAL FILTERING
# ============================================================
log_section "G. Spatial Filtering"

# G1: Bounding box
log_test "G1: Bounding box filter"
RESPONSE=$(auth_request GET "/signalk/v1/history/values?duration=24h&paths=navigation.position&bbox=-74.5,40.2,-73.8,40.9")
check_response "$RESPONSE" "G1: Bounding box" "range"

# G2: Radius filter
log_test "G2: Radius filter (100m)"
RESPONSE=$(auth_request GET "/signalk/v1/history/values?duration=24h&paths=navigation.position&radius=${TEST_LAT},${TEST_LON},100")
check_response "$RESPONSE" "G2: Radius filter" "range"

# G3: Spatial + non-position path
log_test "G3: Spatial correlation (wind data by location)"
RESPONSE=$(auth_request GET "/signalk/v1/history/values?duration=24h&paths=${TEST_PATH}&radius=${TEST_LAT},${TEST_LON},100")
check_response "$RESPONSE" "G3: Spatial correlation" "range"

# G4: Bounding box with non-position path
log_test "G4: Bounding box with non-position path"
RESPONSE=$(auth_request GET "/signalk/v1/history/values?duration=7d&paths=${TEST_PATH}&bbox=-74.0,40.6,-73.9,40.7")
check_response "$RESPONSE" "G4: Bbox + non-position" "range"

# ============================================================
# H. V1 VS V2 API EQUIVALENCE
# ============================================================
log_section "H. V1 vs V2 API Equivalence"

# H1: Values endpoint comparison (V2 API requires ISO 8601 duration format)
log_test "H1: Values endpoint - V1"
V1_RESPONSE=$(auth_request GET "/signalk/v1/history/values?duration=30m&paths=${TEST_PATH}")
check_response "$V1_RESPONSE" "H1a: V1 values endpoint" "range"

log_test "H1: Values endpoint - V2"
V2_RESPONSE=$(auth_request GET "/signalk/v2/api/history/values?duration=PT30M&paths=${TEST_PATH}")
check_response "$V2_RESPONSE" "H1b: V2 values endpoint" "range"

# H2: Contexts comparison
log_test "H2: Contexts - V1 vs V2"
V1_CONTEXTS=$(auth_request GET "/signalk/v1/history/contexts?duration=1h")
V2_CONTEXTS=$(auth_request GET "/signalk/v2/api/history/contexts?duration=PT1H")

if [[ "$V1_CONTEXTS" == "$V2_CONTEXTS" ]]; then
    log_pass "H2: V1/V2 contexts identical"
else
    # Check if both are valid JSON with same structure
    V1_SORTED=$(echo "$V1_CONTEXTS" | jq -S '.' 2>/dev/null || echo "invalid")
    V2_SORTED=$(echo "$V2_CONTEXTS" | jq -S '.' 2>/dev/null || echo "invalid")
    if [[ "$V1_SORTED" == "$V2_SORTED" ]]; then
        log_pass "H2: V1/V2 contexts equivalent"
    else
        log_fail "H2: V1/V2 contexts differ" "Responses not identical"
    fi
fi

# H3: Paths comparison
# Note: V1 and V2 may use different duration parsing, so we test them separately
log_test "H3: V1 paths endpoint"
V1_PATHS=$(auth_request GET "/signalk/v1/history/paths?duration=1h")
check_response "$V1_PATHS" "H3a: V1 paths"

log_test "H3: V2 paths endpoint"
V2_PATHS=$(auth_request GET "/signalk/v2/api/history/paths?duration=PT1H")
check_response "$V2_PATHS" "H3b: V2 paths"

# ============================================================
# I. ALTERNATIVE API ROUTES (registered on main app router)
# ============================================================
log_section "I. Alternative API Routes"

# I1: Alternative values endpoint (registered on main app)
log_test "I1: Alternative values endpoint (/api/history/values)"
RESPONSE=$(auth_request GET "/api/history/values?duration=1h&paths=${TEST_PATH}")
check_response "$RESPONSE" "I1: /api/history values" "range"

# I2: Alternative contexts endpoint
log_test "I2: Alternative contexts endpoint (/api/history/contexts)"
RESPONSE=$(auth_request GET "/api/history/contexts?duration=1h")
check_response "$RESPONSE" "I2: /api/history contexts"

# I3: Alternative paths endpoint
log_test "I3: Alternative paths endpoint (/api/history/paths)"
RESPONSE=$(auth_request GET "/api/history/paths?duration=1h")
check_response "$RESPONSE" "I3: /api/history paths"

# ============================================================
# J. DATA QUERY ENDPOINTS
# ============================================================
log_section "J. Data Query Endpoints"

# J1: List files for a path
# Note: files array may be empty if data is still in SQLite buffer
log_test "J1: List files for path"
RESPONSE=$(auth_request GET "/plugins/signalk-parquet/api/files/${TEST_PATH}")
if echo "$RESPONSE" | jq -e '.success' > /dev/null 2>&1; then
    log_pass "J1: List files"
else
    check_response "$RESPONSE" "J1: List files"
fi

# J2: Sample data from a path
# Note: This may return "No parquet files found" if data is still in SQLite buffer
log_test "J2: Sample data from path"
RESPONSE=$(auth_request GET "/plugins/signalk-parquet/api/sample/${TEST_PATH}")
if echo "$RESPONSE" | jq -e '.error' > /dev/null 2>&1; then
    ERROR_MSG=$(echo "$RESPONSE" | jq -r '.error')
    if [[ "$ERROR_MSG" == *"No parquet files found"* ]]; then
        log_pass "J2: Sample data (no parquet files yet - data in buffer)"
    else
        log_fail "J2: Sample data" "$ERROR_MSG"
    fi
else
    check_response "$RESPONSE" "J2: Sample data"
fi

# J3: Execute SQL query
# Note: This may fail if no parquet files exist yet (data still in SQLite buffer)
log_test "J3: Execute SQL query"
RESPONSE=$(auth_request POST "/plugins/signalk-parquet/api/query" \
    '{"query": "SELECT COUNT(*) as count FROM read_parquet('\''tier=raw/**/*.parquet'\'', union_by_name=true) LIMIT 10"}')
if echo "$RESPONSE" | jq -e '.error' > /dev/null 2>&1; then
    ERROR_MSG=$(echo "$RESPONSE" | jq -r '.error')
    if [[ "$ERROR_MSG" == *"No files found"* ]]; then
        log_pass "J3: SQL query (no parquet files yet - data in buffer)"
    else
        log_fail "J3: SQL query" "$ERROR_MSG"
    fi
else
    check_response "$RESPONSE" "J3: SQL query"
fi

# ============================================================
# SUMMARY
# ============================================================
log_section "TEST SUMMARY"

TOTAL=$((PASSED + FAILED + SKIPPED))

echo ""
echo -e "Total Tests: ${TOTAL}"
echo -e "${GREEN}Passed: ${PASSED}${NC}"
echo -e "${RED}Failed: ${FAILED}${NC}"
echo -e "${YELLOW}Skipped: ${SKIPPED}${NC}"
echo ""

# Calculate percentage
if [[ $TOTAL -gt 0 ]]; then
    PERCENT=$((PASSED * 100 / TOTAL))
    echo -e "Success Rate: ${PERCENT}%"
fi

echo ""
echo "Detailed Results:"
echo "================="
for result in "${RESULTS[@]}"; do
    if [[ "$result" == PASS* ]]; then
        echo -e "${GREEN}$result${NC}"
    elif [[ "$result" == FAIL* ]]; then
        echo -e "${RED}$result${NC}"
    else
        echo -e "${YELLOW}$result${NC}"
    fi
done

echo ""

# Exit with appropriate code
if [[ $FAILED -gt 0 ]]; then
    echo -e "${RED}Some tests failed!${NC}"
    exit 1
else
    echo -e "${GREEN}All tests passed!${NC}"
    exit 0
fi
