//! Memory and performance benchmarks for oxbow-sqs crate
//!
//! This benchmark evaluates:
//! - Message throughput under different configuration
//! - Memory allocation patterns during message processing
//! - Performance characteristics with varying batch sizes

use criterion::{BenchmarkId, Criterion, black_box, criterion_group, criterion_main};
use oxbow_sqs::{ConsumerConfig, TimedConsumer};
use std::time::{Duration, Instant};
use tokio::runtime::Runtime;
use url::Url;

const MOCK_QUEUE_URL: &str = "https://sqs.us-east-1.amazonaws.com/123456789012/test-queue";

fn mock_aws_config() -> aws_config::SdkConfig {
    let runtime = Runtime::new().expect("Failed to create tokio runtime");
    runtime.block_on(async {
        aws_config::from_env()
            .endpoint_url("http://localhost:4566") // Using localstack for mock
            .load()
            .await
    })
}

fn memory_consumption_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("Memory Consumption");

    // Test different message sizes
    let message_sizes = [128, 1024, 4096, 16384]; // bytes

    for size in message_sizes {
        group.bench_with_input(
            BenchmarkId::new("Memory Consumption", size),
            &size,
            |b, &size| {
                b.iter(|| {
                    // Simplified memory test without stats_alloc
                    let mut data = vec![0u8; size * 100];
                    black_box(&mut data);
                });
            },
        );
    }

    group.finish();
}

fn throughput_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("Throughput");

    // Test different message batch sizes
    let batch_sizes = [1, 10, 50, 100];

    for batch_size in batch_sizes {
        group.bench_with_input(
            BenchmarkId::new("Throughput", batch_size),
            &batch_size,
            |b, &batch_size| {
                b.iter(|| {
                    let runtime = Runtime::new().expect("Failed to create tokio runtime");
                    runtime.block_on(async {
                        let config = ConsumerConfig {
                            queue: Url::parse(MOCK_QUEUE_URL).unwrap(),
                            retrieval_max: batch_size,
                        };

                        let mut timed_consumer = TimedConsumer::new(
                            config,
                            &mock_aws_config(),
                            Duration::from_millis(1000),
                        );

                        // Measure time to "process" batches
                        let start_time = Instant::now();
                        let mut total_messages = 0;

                        while let Ok(Some(messages)) = timed_consumer.next().await {
                            total_messages += messages.len();

                            // Simulate work
                            for message in messages {
                                black_box(message.body());
                            }

                            if start_time.elapsed() >= Duration::from_secs(1) {
                                break;
                            }
                        }

                        black_box(total_messages);
                    });
                });
            },
        );
    }

    group.finish();
}

fn message_lifecycle_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("Message Lifecycle");

    // Test full lifecycle: receive, process, delete
    group.bench_function("Full Message Lifecycle", |b| {
        b.iter(|| {
            let runtime = Runtime::new().expect("Failed to create tokio runtime");
            runtime.block_on(async {
                let config = ConsumerConfig::default();
                let mut timed_consumer =
                    TimedConsumer::new(config, &mock_aws_config(), Duration::from_secs(10));

                // Receive messages
                let messages = timed_consumer.next().await.expect("Failed to get messages");

                // Process messages (just touch them)
                if let Some(msg_batch) = messages {
                    for message in msg_batch {
                        black_box(message.body());
                    }
                }

                // Flush (delete)
                timed_consumer.flush().await.expect("Failed to flush");
            });
        });
    });

    group.finish();
}

criterion_group! {
    name = benches;
    config = Criterion::default().warm_up_time(Duration::from_secs(3));
    targets = memory_consumption_benchmark, throughput_benchmark, message_lifecycle_benchmark
}

criterion_main!(benches);
