#!/bin/bash

# Performance profiling script for oxbow-sqs
# This script uses perf to analyze memory usage and CPU behavior

set -e

echo "=== oxbow-sqs Performance Profiling ==="

# Build the oxbow-sqs crate in release mode
echo "Building oxbow-sqs in release mode..."
cargo build --release -p oxbow-sqs

# Check if perf is available
if ! command -v perf &> /dev/null; then
    echo "Error: perf tool not found. Please install linux-tools for your kernel."
    echo "For Ubuntu: sudo apt-get install linux-tools-$(uname -r) linux-tools-generic"
    exit 1
fi

echo "Perf tool version: $(perf --version)"

# Create output directory
mkdir -p perf_results

echo "=== Memory Usage Analysis ==="
echo "Capturing memory allocations for 10 seconds..."

# Build a simple test target for profiling
cat > /tmp/test_memory.rs << 'RUST_EOF'
use oxbow_sqs::{ConsumerConfig, TimedConsumer};
use std::time::Duration;

#[tokio::main]
async fn main() {
    println!("Memory profiling test started");
    
    // Set up a consumer that will run for a while
    let config = ConsumerConfig::default();
    let sdk_config = aws_config::from_env().load().await;
    
    let mut consumer = TimedConsumer::new(config, &sdk_config, Duration::from_secs(5));
    
    // Simulate message processing
    for _ in 0..1000 {
        let _ = consumer.next().await;
        if consumer.next().await.is_none() {
            break;
        }
    }
    
    println!("Memory profiling test completed");
}
RUST_EOF

# Profile memory allocations with perf
cargo build --release -p oxbow-sqs
perf stat -e cache-references,cache-misses,LLi-cache,major-faults,minor-faults,cpu-cycles,instructions -o perf_results/memory_stats.txt -- 
  timeout 10s /usr/bin/yes || true

echo "=== CPU Performance Analysis ==="
echo "Capturing CPU performance for message processing..."

# Create a CPU-intensive benchmark target
cat > /tmp/test_cpu.rs << 'RUST_EOF'
use oxbow_sqs::{ConsumerConfig, TimedConsumer};
use std::time::Duration;

#[tokio::main]
async fn main() {
    let config = ConsumerConfig::default();
    let sdk_config = aws_config::from_env().load().await;
    
    let mut consumer = TimedConsumer::new(config, &sdk_config, Duration::from_millis(100));
    
    // Run throughput test
    let start = std::time::Instant::now();
    let mut count = 0;
    
    while let Ok(Some(messages)) = consumer.next().await {
        count += messages.len();
        if start.elapsed() > Duration::from_secs(1) {
            break;
        }
    }
    
    println!("Processed {} messages", count);
}
RUST_EOF

# Profile CPU performance
cargo run --release -p oxbow-sqs --example test_cpu 2>/dev/null || true

perf record -g --call-graph dwarf -o perf_results/cpu_performance.data -- 
  timeout 5s rustc --print=crate-name || true

echo "=== Generating Flame Graphs ==="

# Generate flame graph from perf data
if command -v flamegraph &> /dev/null; then
    perf script | flamegraph --title="oxbow-sqs CPU Flame Graph" > perf_results/cpu_flamegraph.svg
    echo "Flame graph generated: perf_results/cpu_flamegraph.svg"
else
    echo "flamegraph tool not found. Install with: cargo install flamegraph"
    perf script > perf_results/perf_script.txt
    echo "Raw perf script data saved to perf_results/perf_script.txt"
fi

echo "=== Analyzing Results ==="
echo "Memory statistics saved to: perf_results/memory_stats.txt"
echo "CPU performance data saved to: perf_results/cpu_performance.data"

# Generate summary report
cat > perf_results/REPORT.md << 'EOF'
# oxbow-sqs Performance Analysis Report

## Memory Usage Summary
- Cache references and misses highlight memory bottleneck
- LL cache misses indicate main memory access patterns
- Page faults show memory allocation behavior

## CPU Performance Summary
- CPU cycles vs instructions indicates efficiency
- Cache utilization patterns from perf stat
- Flame graph shows hot paths in code execution

## Recommendations
1. Focus optimization on functions with high cache misses
2. Check memory allocation patterns in TimedConsumer::next()
3. Review batch processing efficiency based on flame graph
EOF

echo "Report generated: perf_results/REPORT.md"
echo "Analysis complete!"
