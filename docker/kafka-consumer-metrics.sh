#!/bin/bash

# Script to measure Kafka consumer consumption metrics
# Shows total messages consumed, lag, and consumption rate

if [[ $# -lt 1 ]] ; then
    echo "Usage: $0 <consumer-group-name> [broker_id] [--json]"
    echo ""
    echo "Examples:"
    echo "  $0 consume"
    echo "  $0 pipe"
    echo "  $0 pipe 2"
    echo "  $0 pipe 2 --json"
    echo ""
    echo "This script measures:"
    echo "  - Total messages consumed by the consumer group"
    echo "  - Total lag (messages waiting to be consumed)"
    echo "  - Per-topic breakdown"
    echo "  - Consumer group state"
    echo ""
    echo "Note: broker_id is a number 1-6, which maps to container kafka-X"
    exit 1
fi

consumer_group="$1"
broker_id="${2:-1}"
broker_name="kafka-${broker_id}"
brokers="kafka-1:9092,kafka-2:9092,kafka-3:9092,kafka-4:9092,kafka-5:9092"
json_output=false

if [[ "$3" == "--json" ]]; then
    json_output=true
fi

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo "========================================"
echo "Kafka Consumer Metrics"
echo "========================================"
echo "Consumer Group: $consumer_group"
echo "Brokers: $brokers"
echo "========================================"
echo ""

# Get consumer group details
output=$(docker exec ${broker_name} kafka-consumer-groups \
    --bootstrap-server ${brokers} \
    --command-config /etc/kafka/secrets/admin-scram.properties \
    --describe --group "$consumer_group" 2>&1)

if [[ $? -ne 0 ]] || [[ "$output" == *"does not exist"* ]]; then
    echo -e "${RED}ERROR: Consumer group '$consumer_group' not found or error occurred${NC}"
    echo "$output"
    exit 1
fi

# Check if consumer group has any offsets
if [[ "$output" == *"Consumer group"* ]] && [[ "$output" != *"TOPIC"* ]]; then
    echo -e "${YELLOW}WARNING: Consumer group exists but has no offset information${NC}"
    echo "This usually means the consumer hasn't committed any offsets yet."
    exit 0
fi

# Get consumer group state
state_output=$(docker exec ${broker_name} kafka-consumer-groups \
    --bootstrap-server ${brokers} \
    --command-config /etc/kafka/secrets/admin-scram.properties \
    --describe --group "$consumer_group" --state 2>&1)

echo "--- Consumer Group State ---"
echo "$state_output"
echo ""

# Parse and calculate metrics
echo "--- Consumption Metrics ---"
echo ""

# Store metrics in arrays for processing
declare -A topic_consumed
declare -A topic_lag
declare -A topic_partitions

total_consumed=0
total_lag=0
total_log_end=0

# Parse the output line by line
while IFS= read -r line; do
    # Skip header and empty lines
    if [[ "$line" == GROUP* ]] || [[ -z "$line" ]] || [[ "$line" == *"Warning"* ]]; then
        continue
    fi

    # Parse the line (format: GROUP TOPIC PARTITION CURRENT-OFFSET LOG-END-OFFSET LAG ...)
    read -r group topic partition current_offset log_end_offset lag rest <<< "$line"

    # Check if we have valid numeric values
    if [[ "$current_offset" =~ ^[0-9]+$ ]] && [[ "$log_end_offset" =~ ^[0-9]+$ ]]; then
        # Calculate lag if not provided or invalid
        if [[ ! "$lag" =~ ^[0-9]+$ ]]; then
            lag=$((log_end_offset - current_offset))
        fi

        # Accumulate totals
        total_consumed=$((total_consumed + current_offset))
        total_lag=$((total_lag + lag))
        total_log_end=$((total_log_end + log_end_offset))

        # Accumulate per-topic metrics
        if [[ -z "${topic_consumed[$topic]}" ]]; then
            topic_consumed[$topic]=0
            topic_lag[$topic]=0
            topic_partitions[$topic]=0
        fi

        topic_consumed[$topic]=$((${topic_consumed[$topic]} + current_offset))
        topic_lag[$topic]=$((${topic_lag[$topic]} + lag))
        topic_partitions[$topic]=$((${topic_partitions[$topic]} + 1))
    fi
done <<< "$output"

# Output results
if [[ $json_output == true ]]; then
    # JSON output format
    echo "{"
    echo "  \"consumer_group\": \"$consumer_group\","
    echo "  \"total_consumed\": $total_consumed,"
    echo "  \"total_lag\": $total_lag,"
    echo "  \"total_available\": $total_log_end,"
    echo "  \"topics\": {"

    first_topic=true
    for topic in "${!topic_consumed[@]}"; do
        if [[ $first_topic == false ]]; then
            echo ","
        fi
        first_topic=false

        consumed=${topic_consumed[$topic]}
        lag=${topic_lag[$topic]}
        partitions=${topic_partitions[$topic]}

        echo -n "    \"$topic\": {"
        echo -n "\"consumed\": $consumed, \"lag\": $lag, \"partitions\": $partitions"
        echo -n "}"
    done

    echo ""
    echo "  }"
    echo "}"
else
    # Human-readable output
    echo -e "${BLUE}Overall Statistics:${NC}"
    echo -e "  Total Messages Consumed:  ${GREEN}$total_consumed${NC}"
    echo -e "  Total Messages Available: $total_log_end"
    echo -e "  Total Lag:                ${YELLOW}$total_lag${NC}"

    if [[ $total_log_end -gt 0 ]]; then
        percentage=$((total_consumed * 100 / total_log_end))
        echo -e "  Consumption Percentage:   ${percentage}%"
    fi

    echo ""
    echo -e "${BLUE}Per-Topic Breakdown:${NC}"

    for topic in "${!topic_consumed[@]}"; do
        consumed=${topic_consumed[$topic]}
        lag=${topic_lag[$topic]}
        partitions=${topic_partitions[$topic]}

        echo ""
        echo "  Topic: $topic"
        echo "    Partitions:        $partitions"
        echo -e "    Messages Consumed: ${GREEN}$consumed${NC}"
        echo -e "    Lag:               ${YELLOW}$lag${NC}"

        if [[ $lag -eq 0 ]]; then
            echo -e "    Status:            ${GREEN}✓ Caught up${NC}"
        elif [[ $lag -lt 1000 ]]; then
            echo -e "    Status:            ${GREEN}✓ Low lag${NC}"
        elif [[ $lag -lt 10000 ]]; then
            echo -e "    Status:            ${YELLOW}⚠ Moderate lag${NC}"
        else
            echo -e "    Status:            ${RED}✗ High lag!${NC}"
        fi
    done
fi

echo ""
echo "--- Detailed Consumer Group Information ---"
echo "$output"

echo ""
echo "========================================"
echo "Tip: Run this script periodically to monitor consumption rate"
echo "========================================"

