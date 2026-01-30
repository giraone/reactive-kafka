#!/bin/bash

# Script to test if a consumer group has successfully read N messages
# Tracks both producer (LEO) and consumer offsets to identify where issues occur
# Usage: Run twice - first to capture baseline, second to show progress

SCRIPT_NAME=$(basename "$0")
CACHE_DIR="/tmp/kafka-consumer-offsets"
mkdir -p "$CACHE_DIR"

if [[ $# -lt 2 ]] ; then
    echo "Usage: $0 <capture|compare> <consumer-group-name> [expected_count]"
    echo ""
    echo "Examples:"
    echo "  # Step 1: Capture initial offsets (both producer LEO and consumer offsets)"
    echo "  $0 capture b1"
    echo ""
    echo "  # Step 2: Compare after consuming (shows producer and consumer progress)"
    echo "  $0 compare b1 100"
    echo ""
    echo "Actions:"
    echo "  capture  - Save current consumer group offsets and log end offsets (LEO) as baseline"
    echo "  compare  - Compare current offsets with baseline and identify producer/consumer issues"
    echo ""
    echo "Parameters:"
    echo "  expected_count  - Expected number of messages to consume (optional)"
    echo ""
    echo "Diagnostics:"
    echo "  - Messages produced but not consumed → Consumer issue"
    echo "  - Messages not produced → Producer issue"
    echo "  - Consumer offset higher than LEO (log end offset) → Consumer read messages twice"
    exit 1
fi

broker="kafka-1"
brokers="kafka-1:9092,kafka-2:9092,kafka-3:9092,kafka-4:9092,kafka-5:9092,kafka-6:9092"

action="$1"
consumer_group="$2"
expected_count="${3:-}"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
MAGENTA='\033[0;35m'
NC='\033[0m' # No Color

cache_file="$CACHE_DIR/${consumer_group}.offsets"

# Function to get current consumer offsets and LEO (Log End Offset)
get_offsets() {
    docker exec ${broker} kafka-consumer-groups \
        --bootstrap-server ${brokers} \
        --command-config /etc/kafka/secrets/admin-scram.properties \
        --describe --group "$consumer_group" 2>&1 | \
        awk 'NR>1 && $1!="" && $3~/^[0-9]+$/ && $4~/^[0-9]+$/ {print $2 ":" $3 ":" $4 ":" $5}'
}

# Function to parse offset line (topic:partition:current_offset:leo)
parse_offset_line() {
    local line="$1"
    IFS=':' read -r topic partition offset leo <<< "$line"
    echo "$topic" "$partition" "$offset" "$leo"
}

if [[ "$action" == "capture" ]]; then
    echo "========================================"
    echo "Capturing Consumer Group Offsets & LEO"
    echo "========================================"
    echo "Consumer Group: $consumer_group"
    echo "========================================"
    echo ""

    # Get current offsets (includes LEO)
    offsets=$(get_offsets)

    if [[ -z "$offsets" ]]; then
        echo -e "${RED}ERROR: Could not retrieve offsets for consumer group '$consumer_group'${NC}"
        echo "Make sure the consumer group exists and has committed offsets."
        exit 1
    fi

    # Save to cache files
    echo "$offsets" > "$cache_file"

    echo -e "${GREEN}✓ Baseline offsets captured successfully${NC}"
    echo ""
    echo "Captured offsets:"
    echo "----------------------------------------"
    printf "%-40s %-10s %-15s %-15s %-10s\n" "TOPIC" "PARTITION" "CONSUMER-OFFSET" "LOG-END-OFFSET" "LAG"
    echo "----------------------------------------"

    total_offset=0
    total_leo=0
    total_lag=0
    while IFS= read -r line; do
        read -r topic partition offset leo <<< "$(parse_offset_line "$line")"
        lag=$((leo - offset))
        printf "%-40s %-10s %-15s %-15s %-10s\n" "$topic" "$partition" "$offset" "$leo" "$lag"
        total_offset=$((total_offset + offset))
        total_leo=$((total_leo + leo))
        total_lag=$((total_lag + lag))
    done <<< "$offsets"

    echo "----------------------------------------"
    echo "Total consumer offset: $total_offset"
    echo "Total log end offset (LEO): $total_leo"
    echo "Total lag: $total_lag"
    echo ""
    echo -e "${BLUE}Next step: Run your producer and consumer, then execute:${NC}"
    echo "  $SCRIPT_NAME compare $consumer_group [expected_count]"

elif [[ "$action" == "compare" ]]; then
    echo "========================================"
    echo "Comparing Consumer Group Progress"
    echo "========================================"
    echo "Consumer Group: $consumer_group"
    echo "========================================"
    echo ""

    # Check if baseline exists
    if [[ ! -f "$cache_file" ]]; then
        echo -e "${RED}ERROR: No baseline found for consumer group '$consumer_group'${NC}"
        echo "Run '$SCRIPT_NAME capture $consumer_group' first to capture baseline offsets."
        exit 1
    fi

    # Load baseline offsets
    baseline_offsets=$(cat "$cache_file")

    # Get current offsets
    current_offsets=$(get_offsets)

    if [[ -z "$current_offsets" ]]; then
        echo -e "${RED}ERROR: Could not retrieve current offsets for consumer group '$consumer_group'${NC}"
        exit 1
    fi


    # Create associative arrays for baseline
    declare -A baseline_consumer_map
    declare -A baseline_leo_map
    while IFS= read -r line; do
        read -r topic partition offset leo <<< "$(parse_offset_line "$line")"
        key="${topic}:${partition}"
        baseline_consumer_map[$key]=$offset
        baseline_leo_map[$key]=$leo
    done <<< "$baseline_offsets"

    # Compare and calculate differences
    declare -A topic_consumed
    declare -A topic_produced

    total_consumed=0
    total_produced=0
    partition_count=0

    # Track issues
    has_producer_issue=0
    has_consumer_issue=0
    has_duplicate_read=0

    echo "Progress Details (CONS=Consumed, LEO=Log End Offset=Produced):"
    echo "----------------------------------------"
    printf "%-40s %-5s %-10s %-10s %-10s %-10s %-10s %-15s\n" "TOPIC" "PART" "CONS-OLD" "CONS-NEW" "LEO-OLD" "LEO-NEW" "PRODUCED" "CONSUMED"
    echo "----------------------------------------"

    while IFS= read -r line; do
        read -r topic partition current_consumer_offset current_leo <<< "$(parse_offset_line "$line")"
        key="${topic}:${partition}"

        baseline_consumer_offset=${baseline_consumer_map[$key]:-0}
        baseline_leo_offset=${baseline_leo_map[$key]:-0}

        # Calculate changes
        consumed=$((current_consumer_offset - baseline_consumer_offset))
        produced=$((current_leo - baseline_leo_offset))

        printf "%-40s %-5s %-10s %-10s %-10s %-10s " "$topic" "$partition" "$baseline_consumer_offset" "$current_consumer_offset" "$baseline_leo_offset" "$current_leo"

        # Color code produced messages
        if [[ $produced -gt 0 ]]; then
            printf "${GREEN}%-10s${NC} " "+$produced"
        elif [[ $produced -eq 0 ]]; then
            printf "${YELLOW}%-10s${NC} " "0"
        else
            printf "${RED}%-10s${NC} " "$produced"
        fi

        # Color code consumed messages and add diagnostics
        if [[ $consumed -gt 0 ]]; then
            printf "${GREEN}%-15s${NC}" "+$consumed"
            if [[ $consumed -gt $produced ]]; then
                printf " ${MAGENTA}⚠ Consumed more than produced!${NC}"
                has_duplicate_read=1
            elif [[ $consumed -lt $produced ]]; then
                printf " ${YELLOW}⚠ Not all produced messages consumed${NC}"
                has_consumer_issue=1
            fi
        elif [[ $consumed -eq 0 ]]; then
            printf "${YELLOW}%-15s${NC}" "0"
            if [[ $produced -gt 0 ]]; then
                printf " ${RED}✗ Messages produced but not consumed${NC}"
                has_consumer_issue=1
            fi
        else
            printf "${RED}%-15s${NC}" "$consumed"
            printf " ${RED}✗ Negative consumption (offset reset?)${NC}"
            has_consumer_issue=1
        fi
        echo ""

        total_consumed=$((total_consumed + consumed))
        total_produced=$((total_produced + produced))
        partition_count=$((partition_count + 1))

        # Accumulate per-topic stats
        if [[ -z "${topic_consumed[$topic]}" ]]; then
            topic_consumed[$topic]=0
            topic_produced[$topic]=0
        fi
        topic_consumed[$topic]=$((${topic_consumed[$topic]} + consumed))
        topic_produced[$topic]=$((${topic_produced[$topic]} + produced))

    done <<< "$current_offsets"

    echo "----------------------------------------"
    echo ""

    # Per-topic summary
    if [[ ${#topic_consumed[@]} -gt 1 ]]; then
        echo "Per-Topic Summary:"
        echo "----------------------------------------"
        for topic in "${!topic_consumed[@]}"; do
            consumed=${topic_consumed[$topic]}
            produced=${topic_produced[$topic]}
            echo -e "  ${CYAN}$topic${NC}:"
            echo -e "    Produced: ${GREEN}+${produced}${NC} messages"
            echo -e "    Consumed: ${GREEN}+${consumed}${NC} messages"
        done
        echo "----------------------------------------"
        echo ""
    fi

    # Overall summary
    echo "========================================"
    echo "Overall Summary:"
    echo "========================================"

    if [[ total_produced -eq $expected_count ]] && [[ -n "$expected_count" ]]; then
      echo -e "Messages Produced (by producer): ${GREEN}${total_produced}${NC}"
    elif [[ total_produced -gt $expected_count ]] && [[ -n "$expected_count" ]]; then
      echo -e "Messages Produced (by producer): ${YELLOW}${total_produced}${NC}"
    else
      echo -e "Messages Produced (by producer): ${RED}${total_produced}${NC}"
    fi

    if [[ $total_consumed -eq $expected_count ]] && [[ -n "$expected_count" ]]; then
      echo -e "Messages Consumed (by consumer): ${GREEN}${total_consumed}${NC}"
    elif [[ $total_consumed -gt $expected_count ]] && [[ -n "$expected_count" ]]; then
      echo -e "Messages Consumed (by consumer): ${YELLOW}${total_consumed}${NC}"
    else
      echo -e "Messages Consumed (by consumer): ${RED}${total_consumed}${NC}"
    fi
    echo "Partitions Monitored: $partition_count"

    if [[ -n "$expected_count" ]]; then
        echo "Expected Messages: $expected_count"
    fi
    echo ""

    # Diagnostic summary
    echo "========================================"
    echo "Diagnostic Summary:"
    echo "========================================"

    if [[ $total_produced -eq 0 ]]; then
        echo -e "${RED}✗ PRODUCER ISSUE: No messages were produced to the topic${NC}"
        echo "  → Check if your producer is running and configured correctly"
        has_producer_issue=1
    elif [[ $total_produced -gt 0 ]]; then
        echo -e "${GREEN}✓ Producer working: $total_produced messages produced${NC}"
    fi

    if [[ $total_consumed -eq 0 ]] && [[ $total_produced -gt 0 ]]; then
        echo -e "${RED}✗ CONSUMER ISSUE: Messages were produced but not consumed${NC}"
        echo "  → Check if your consumer is running and configured correctly"
        echo "  → Verify consumer group name and topic subscription"
        has_consumer_issue=1
    elif [[ $total_consumed -lt $total_produced ]]; then
        diff=$((total_produced - total_consumed))
        echo -e "${YELLOW}⚠ CONSUMER ISSUE: Consumer is lagging behind${NC}"
        echo "  → $diff messages produced but not yet consumed"
        echo "  → Consumer may be slow or not processing all messages"
        has_consumer_issue=1
    elif [[ $total_consumed -gt $total_produced ]]; then
        diff=$((total_consumed - total_produced))
        echo -e "${MAGENTA}⚠ CONSUMER ISSUE: Consumer read more than produced${NC}"
        echo "  → Consumer read $diff more messages than were produced in this period"
        echo "  → This may indicate duplicate processing or offset management issues"
        has_duplicate_read=1
    elif [[ $total_consumed -eq $total_produced ]] && [[ $total_consumed -gt 0 ]]; then
        echo -e "${GREEN}✓ Consumer working: All produced messages were consumed${NC}"
    fi

    echo "========================================"
    echo ""

    # Final verdict based on expected count
    if [[ -n "$expected_count" ]]; then
        if [[ $total_consumed -eq $expected_count ]] && [[ $total_produced -eq $expected_count ]]; then
            echo -e "${GREEN}✓ SUCCESS: Produced and consumed exactly $expected_count messages!${NC}"
            exit 0
        elif [[ $total_produced -lt $expected_count ]]; then
            diff=$((expected_count - total_produced))
            echo -e "${RED}✗ FAILED: Producer only produced $total_produced messages (expected $expected_count)${NC}"
            echo -e "${RED}  Missing $diff messages - PRODUCER ISSUE${NC}"
            exit 1
        elif [[ $total_consumed -lt $expected_count ]]; then
            diff=$((expected_count - total_consumed))
            echo -e "${RED}✗ FAILED: Consumer only consumed $total_consumed messages (expected $expected_count)${NC}"
            if [[ $total_produced -eq $expected_count ]]; then
                echo -e "${RED}  Producer worked correctly - CONSUMER ISSUE${NC}"
            else
                echo -e "${RED}  Missing $diff messages - check both producer and consumer${NC}"
            fi
            exit 1
        else
            if [[ $total_produced -gt $expected_count ]]; then
                diff=$((total_produced - expected_count))
                echo -e "${YELLOW}⚠ WARNING: Produced $diff more messages than expected${NC}"
                echo -e "${YELLOW}  This may indicate duplicate writes - PRODUCER ISSUE${NC}"
                exit 1
            fi
            if [[ $total_consumed -gt $expected_count ]]; then
                diff=$((total_consumed - expected_count))
                echo -e "${YELLOW}⚠ WARNING: Consumed $diff more messages than expected${NC}"
                echo -e "${YELLOW}  This may indicate duplicate reads - CONSUMER ISSUE${NC}"
                exit 1
            fi
            exit 0
        fi
    else
        if [[ $has_producer_issue -eq 1 ]]; then
            echo -e "${RED}✗ FAILED: Producer issues detected${NC}"
            exit 1
        elif [[ $has_consumer_issue -eq 1 ]] || [[ $has_duplicate_read -eq 1 ]]; then
            echo -e "${YELLOW}⚠ WARNING: Consumer issues detected${NC}"
            exit 1
        elif [[ $total_consumed -gt 0 ]] && [[ $total_consumed -eq $total_produced ]]; then
            echo -e "${GREEN}✓ SUCCESS: Consumer successfully processed all produced messages${NC}"
            exit 0
        elif [[ $total_consumed -eq 0 ]] && [[ $total_produced -eq 0 ]]; then
            echo -e "${YELLOW}⚠ No activity detected (no messages produced or consumed)${NC}"
            exit 0
        fi
    fi

else
    echo -e "${RED}ERROR: Invalid action '$action'${NC}"
    echo "Use 'capture' or 'compare'"
    exit 1
fi

