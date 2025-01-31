#!/bin/bash

# Function to test the processor workflow
test_processor() {
    act workflow_dispatch -W .github/workflows/test_parquet_processor.local.yml \
        -s GITHUB_TOKEN="$(cat .secrets)" \
        -e .github/test_events/processor_event.json \
        --container-architecture linux/amd64 \
        --artifact-server-path=/tmp/artifacts \
        --secret-file .secrets \
        -P ubuntu-latest=python:3.10-slim \
        --bind
}

# Function to test the coordinator workflow
test_coordinator() {
    act workflow_dispatch -W .github/workflows/test_parquet_coordinator.yml \
        -s GITHUB_TOKEN="$(cat .secrets)" \
        -e .github/test_events/coordinator_event.json \
        --container-architecture linux/amd64 \
        --artifact-server-path=/tmp/artifacts
}

# Function to test the aggregator workflow
test_aggregator() {
    act workflow_dispatch -W .github/workflows/test_parquet_aggregator.yml \
        -s GITHUB_TOKEN="$(cat .secrets)" \
        -e .github/test_events/aggregator_event.json \
        --container-architecture linux/amd64 \
        --artifact-server-path=/tmp/artifacts
}

# Create test event files if they don't exist
mkdir -p .github/test_events

# Create processor test event
cat > .github/test_events/processor_event.json <<EOL
{
    "inputs": {
        "start_index": "0",
        "total_processed": "0",
        "batch_size": "50",
        "max_records": "10",
        "segment_name": "test-local",
        "total_target": "100"
    }
}
EOL

# Create coordinator test event
cat > .github/test_events/coordinator_event.json <<EOL
{
    "inputs": {
        "batch_size": "50",
        "segment_size": "1000",
        "total_records": "5000",
        "max_concurrent": "3"
    }
}
EOL

# Create aggregator test event
cat > .github/test_events/aggregator_event.json <<EOL
{
    "inputs": {
        "timestamp": "$(date +%Y-%m-%d_%H-%M-%S)"
    }
}
EOL

# Parse command line arguments
case "$1" in
    "processor")
        test_processor
        ;;
    "coordinator")
        test_coordinator
        ;;
    "aggregator")
        test_aggregator
        ;;
    *)
        echo "Usage: $0 {processor|coordinator|aggregator}"
        echo "Tests GitHub Actions workflows locally using act"
        exit 1
        ;;
esac 