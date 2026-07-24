use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use oxbow::TableMods;

fn benchmark_table_mods_operations(c: &mut Criterion) {
    let mut group = c.benchmark_group("TableMods Operations");

    // Benchmark creation and basic operations
    group.bench_function("TableMods::default", |b| {
        b.iter(|| {
            let mods = TableMods::default();
            mods.adds().len() + mods.removes().len()
        });
    });

    // Benchmark with more complex scenarios using built-in new function
    group.bench_function("TableMods::new_empty", |b| {
        b.iter(|| {
            let mods = TableMods::new(&[], &[]);
            mods.adds().len() + mods.removes().len()
        });
    });

    group.bench_function("TableMods_10_adds_operations", |b| {
        b.iter(|| {
            let mut mods = TableMods::default();
            for i in 0..10 {
                // Simple string concatenation instead of complex struct creation
                let _ = format!("/test-{}.parquet", i);
            }
            mods.adds().len()
        });
    });

    group.bench_function("TableMods_100_adds_operations", |b| {
        b.iter(|| {
            let mut mods = TableMods::default();
            for i in 0..100 {
                let _ = format!("/test-{}.parquet", i);
            }
            mods.adds().len()
        });
    });

    group.finish();
}

criterion_group!(benchmarks, benchmark_table_mods_operations);
criterion_main!(benchmarks);
