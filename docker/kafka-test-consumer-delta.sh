#!/bin/bash

# Script to analyze Kafka read-process-write chains for duplicate and missing messages
# Uses captured baseline data from kafka-test-consumer-progress.sh
# Extracts events and compares message keys across the chain

CACHE_DIR="/tmp/kafka-consumer-offsets"
EVENTS_DIR="/tmp/kafka-consumer-events"
mkdir -p "$EVENTS_DIR"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

if [[ $# -lt 2 ]] || [[ $(($# % 2)) -ne 0 ]]; then
    echo "Usage: $0 <consumer-group-1> <topic-1> [<consumer-group-2> <topic-2> ...]"
    echo ""
    echo "Description:"
    echo "  Analyzes a Kafka read-process-write chain for duplicate and missing messages."
    echo "  Requires captured baseline data from kafka-test-consumer-progress.sh"
    echo ""
    echo "  Each pair represents a step in the chain:"
    echo "    - consumer-group: The consumer group name (used to find captured offsets)"
    echo "    - topic: The topic to analyze"
    echo ""
    echo "Examples:"
    echo "  # Analyze a 2-step chain: topic-a -> process -> topic-b"
    echo "  $0 consumer-a topic-a consumer-b topic-b"
    echo ""
    echo "  # Analyze a 3-step chain: input -> process1 -> intermediate -> process2 -> output"
    echo "  $0 consumer-input topic-input consumer-intermediate topic-intermediate consumer-output topic-output"
    echo ""
    echo "Prerequisite:"
    echo "  Run 'kafka-test-consumer-progress.sh capture <consumer-group>' for each consumer group first."
    echo ""
    echo "Analysis:"
    echo "  1. Extracts events from each topic using captured baseline offsets"
    echo "  2. Detects duplicate messages within each topic (by message key)"
    echo "  3. Detects missing messages between topics in the chain (by message key)"
    exit 1
fi

broker="kafka-1"
brokers="kafka-1:9092,kafka-2:9092,kafka-3:9092,kafka-4:9092,kafka-5:9092"

# Parse arguments into arrays
declare -a consumer_groups
declare -a topics

while [[ $# -gt 0 ]]; do
    consumer_groups+=("$1")
    topics+=("$2")
    shift 2
done

chain_length=${#consumer_groups[@]}

echo "========================================"
echo "Kafka Read-Process-Write Chain Analysis"
echo "========================================"
echo "Chain Length: $chain_length steps"
echo ""

# Display chain
echo "Chain:"
for i in "${!consumer_groups[@]}"; do
    echo "  Step $((i+1)): Consumer Group '${consumer_groups[$i]}' → Topic '${topics[$i]}'"
done
echo "========================================"
echo ""

# Function to check if baseline exists
check_baseline() {
    local consumer_group="$1"
    local cache_file="$CACHE_DIR/${consumer_group}.offsets"

    if [[ ! -f "$cache_file" ]]; then
        echo -e "${RED}ERROR: No baseline found for consumer group '$consumer_group'${NC}"
        echo "Run 'kafka-test-consumer-progress.sh capture $consumer_group' first."
        return 1
    fi
    return 0
}

# Function to extract events from topic starting at captured offsets
extract_events() {
    local consumer_group="$1"
    local topic="$2"
    local cache_file="$CACHE_DIR/${consumer_group}.offsets"
    local output_file="$EVENTS_DIR/${topic}-events.txt"

    echo "Extracting events from topic '$topic'..."

    # Read captured offsets for this consumer group
    if [[ ! -f "$cache_file" ]]; then
        echo -e "${RED}ERROR: No baseline file found: $cache_file${NC}"
        return 1
    fi

    # Parse baseline to get consumer offsets per partition
    # Format: topic:partition:consumer_offset:leo
    true > "$output_file"  # Clear output file

    # We need to consume from each partition separately with specific offsets
    # Unfortunately kafka-console-consumer doesn't support per-partition offset specification easily
    # So we'll consume from beginning and filter based on offset ranges

    echo "  Reading baseline offsets from $cache_file..."

    # Create a temporary config file for partition assignments
    local temp_dir
    temp_dir=$(mktemp -d)
    local partition_list=""

    while IFS=':' read -r baseline_topic partition consumer_offset leo; do
        if [[ "$baseline_topic" == "$topic" ]]; then
            if [[ -n "$partition_list" ]]; then
                partition_list+=","
            fi
            partition_list+="$partition"
            echo "  Partition $partition: consumer_offset=$consumer_offset, leo=$leo"
        fi
    done < "$cache_file"

    if [[ -z "$partition_list" ]]; then
        echo -e "${YELLOW}  No partitions found for topic '$topic' in baseline${NC}"
        rm -rf "$temp_dir"
        return 0
    fi

    # Extract events using kafka-console-consumer
    # We consume from the beginning and use timestamps/offsets to filter
    # For simplicity, we'll consume all and let the offset information help us

    echo "  Consuming events from partitions: $partition_list"

    docker exec ${broker} bash -c "kafka-console-consumer \
        --bootstrap-server ${brokers} \
        --consumer.config /etc/kafka/secrets/admin-scram.properties \
        --topic '${topic}' \
        --property print.key=true \
        --property print.partition=true \
        --property print.timestamp=true \
        --property print.offset=true \
        --from-beginning \
        --timeout-ms 5000 2>/dev/null" | \
    while IFS= read -r line; do
        # Parse the line format: CreateTime:X Partition:Y Offset:Z MessageKey MessageValue
        if [[ $line =~ CreateTime:([0-9]{10})[0-9]+[[:space:]]+Partition:([0-9]+)[[:space:]]+Offset:([0-9]+) ]]; then
            epoch="${BASH_REMATCH[1]}"
            part="${BASH_REMATCH[2]}"
            offset="${BASH_REMATCH[3]}"

            # Convert epoch to human-readable datetime
            datetime=$(date -d "@$epoch" "+%Y%m%d %H:%M:%S")
            # Check if this offset is within our range (captured offset >= captured_consumer_offset)
            # We want events that were consumed after capture point
            while IFS=':' read -r baseline_topic baseline_partition captured_consumer_offset leo; do
                if [[ "$baseline_topic" == "$topic" ]] && [[ "$baseline_partition" == "$part" ]]; then
                    if [[ $offset -ge captured_consumer_offset ]]; then
                        echo "$datetime $line" >> "$output_file"
                    fi
                fi
            done < "$cache_file"
        fi
    done

    rm -rf "$temp_dir"

    local extracted_count=0
    extracted_count=$(wc -l < "$output_file" | tr -d ' ')
    echo -e "  ${GREEN}✓ Extracted $extracted_count events to $output_file${NC}"
    return 0
}

# Function to extract message keys from events file
extract_keys() {
    local events_file="$1"
    local keys_file="$2"

    # Extract keys from format: CreateTime:X Partition:Y Offset:Z MessageKey MessageValue
    awk -F'\t' '{
        # Extract the key part
        key = $4
        if (key != "" && key != "null") {
            print key
        }
    }' "$events_file" | sort > "$keys_file"
}

# Function to detect duplicates in a topic
detect_duplicates() {
    local topic="$1"
    local events_file="$EVENTS_DIR/${topic}-events.txt"
    local keys_file="$EVENTS_DIR/${topic}-keys.txt"
    local duplicates_file="$EVENTS_DIR/${topic}-duplicates.txt"

    if [[ ! -f "$events_file" ]] || [[ ! -s "$events_file" ]]; then
        echo -e "${YELLOW}  No events file or empty for topic '$topic'${NC}"
        return 0
    fi

    extract_keys "$events_file" "$keys_file"

    # Find duplicate keys
    uniq -d "$keys_file" > "$duplicates_file"

    local dup_count
    local total_keys
    dup_count=$(wc -l < "$duplicates_file" | tr -d ' ')
    total_keys=$(wc -l < "$keys_file" | tr -d ' ')

    if [[ $dup_count -gt 0 ]]; then
        echo -e "${RED}  ✗ Found $dup_count duplicate message keys in topic '$topic' (out of $total_keys messages)${NC}"
        echo "    Duplicate keys stored in: $duplicates_file"

        # Show first few duplicates
        if [[ $dup_count -le 10 ]]; then
            echo "    Duplicate keys:"
            while IFS= read -r key; do
                echo "      - $key"
            done < "$duplicates_file"
        else
            echo "    First 10 duplicate keys:"
            head -10 "$duplicates_file" | while IFS= read -r key; do
                echo "      - $key"
            done
            echo "      ... and $((dup_count - 10)) more"
        fi
        return 1
    else
        echo -e "${GREEN}  ✓ No duplicate message keys found in topic '$topic' ($total_keys unique messages)${NC}"
        return 0
    fi
}

# Function to detect missing messages between two topics
detect_missing() {
    local source_topic="$1"
    local target_topic="$2"
    local source_keys="$EVENTS_DIR/${source_topic}-keys.txt"
    local target_keys="$EVENTS_DIR/${target_topic}-keys.txt"
    local missing_file="$EVENTS_DIR/${source_topic}-to-${target_topic}-missing.txt"

    if [[ ! -f "$source_keys" ]] || [[ ! -s "$source_keys" ]]; then
        echo -e "${YELLOW}  No keys file for source topic '$source_topic'${NC}"
        return 0
    fi

    if [[ ! -f "$target_keys" ]] || [[ ! -s "$target_keys" ]]; then
        echo -e "${YELLOW}  No keys file for target topic '$target_topic'${NC}"
        return 0
    fi

    # Find keys in source but not in target (using comm command)
    comm -23 <(sort -u "$source_keys") <(sort -u "$target_keys") > "$missing_file"

    local source_count
    local target_count
    local missing_count
    source_count=$(sort -u "$source_keys" | wc -l | tr -d ' ')
    target_count=$(sort -u "$target_keys" | wc -l | tr -d ' ')
    missing_count=$(wc -l < "$missing_file" | tr -d ' ')

    if [[ $missing_count -gt 0 ]]; then
        echo -e "${RED}  ✗ Found $missing_count messages in '$source_topic' missing from '$target_topic'${NC}"
        echo "    Source messages: $source_count"
        echo "    Target messages: $target_count"
        echo "    Missing keys stored in: $missing_file"

        # Show first few missing keys
        if [[ $missing_count -le 10 ]]; then
            echo "    Missing keys:"
            while IFS= read -r key; do
                echo "      - $key"
            done < "$missing_file"
        else
            echo "    First 10 missing keys:"
            head -10 "$missing_file" | while IFS= read -r key; do
                echo "      - $key"
            done
            echo "      ... and $((missing_count - 10)) more"
        fi
        return 1
    else
        echo -e "${GREEN}  ✓ All messages from '$source_topic' ($source_count) found in '$target_topic' ($target_count)${NC}"
        return 0
    fi
}

# Step 1: Check all baselines exist
echo "Step 1: Checking baselines..."
echo "----------------------------------------"
all_baselines_exist=true
for consumer_group in "${consumer_groups[@]}"; do
    if ! check_baseline "$consumer_group"; then
        all_baselines_exist=false
    fi
done

if [[ "$all_baselines_exist" == false ]]; then
    echo -e "${RED}ERROR: Missing baselines. Cannot proceed.${NC}"
    exit 1
fi
echo -e "${GREEN}✓ All baselines found${NC}"
echo ""

# Step 2: Extract events from all topics
echo "Step 2: Extracting events from topics..."
echo "----------------------------------------"
for i in "${!consumer_groups[@]}"; do
    extract_events "${consumer_groups[$i]}" "${topics[$i]}"
done
echo ""

# Step 3: Detect duplicates in each topic
echo "Step 3: Detecting duplicate messages in each topic..."
echo "----------------------------------------"
has_duplicates=false
for topic in "${topics[@]}"; do
    if ! detect_duplicates "$topic"; then
        has_duplicates=true
    fi
done
echo ""

# Step 4: Detect missing messages between consecutive topics in the chain
echo "Step 4: Detecting missing messages between topics in the chain..."
echo "----------------------------------------"
has_missing=false
for i in $(seq 0 $((chain_length - 2))); do
    source_topic="${topics[$i]}"
    target_topic="${topics[$((i + 1))]}"
    echo "Comparing: '$source_topic' → '$target_topic'"
    if ! detect_missing "$source_topic" "$target_topic"; then
        has_missing=true
    fi
done
echo ""

# Final Summary
echo "========================================"
echo "Analysis Summary"
echo "========================================"
echo "Chain analyzed: $chain_length steps"
echo "Events directory: $EVENTS_DIR"
echo ""

if [[ "$has_duplicates" == false ]] && [[ "$has_missing" == false ]]; then
    echo -e "${GREEN}✓ SUCCESS: No duplicates or missing messages detected!${NC}"
    echo "  - All topics have unique message keys"
    echo "  - All messages successfully propagated through the chain"
    exit 0
elif [[ "$has_duplicates" == true ]] && [[ "$has_missing" == false ]]; then
    echo -e "${YELLOW}⚠ WARNING: Duplicate messages detected${NC}"
    echo "  - Some topics have duplicate message keys"
    echo "  - All messages propagated through the chain (no missing messages)"
    echo "  - Check duplicate files in: $EVENTS_DIR/*-duplicates.txt"
    exit 1
elif [[ "$has_duplicates" == false ]] && [[ "$has_missing" == true ]]; then
    echo -e "${RED}✗ FAILED: Missing messages detected${NC}"
    echo "  - No duplicate message keys found"
    echo "  - Some messages did not propagate through the chain"
    echo "  - Check missing files in: $EVENTS_DIR/*-missing.txt"
    exit 1
else
    echo -e "${RED}✗ FAILED: Both duplicates and missing messages detected${NC}"
    echo "  - Some topics have duplicate message keys"
    echo "  - Some messages did not propagate through the chain"
    echo "  - Check files in: $EVENTS_DIR/"
    exit 1
fi

