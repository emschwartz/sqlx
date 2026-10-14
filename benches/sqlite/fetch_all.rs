//! Per-row materialisation cost of large SQLite result sets.
//!
//! Each benchmark is one `fetch_all` of 10,000 rows from an in-memory database,
//! decoded into a tuple. The three shapes separate per-value overhead on
//! numeric columns from the cost of copying TEXT payloads:
//!
//! - `numeric`: 10 columns, 5 INTEGER + 5 REAL
//! - `mixed`: 10 columns, 6 INTEGER + 2 REAL + 2 TEXT of 20 bytes
//! - `wide_text`: 4 columns, 2 INTEGER + 1 REAL + 1 TEXT of 1,000 bytes
//!
//! Run with `cargo bench --bench sqlite-fetch-all --features sqlite,runtime-tokio`.
//!
//! Set `SQLX_BENCH_SQLITE_MEMSTATUS=0` to call
//! `sqlite3_config(SQLITE_CONFIG_MEMSTATUS, 0)` before the first connection
//! opens, which removes SQLite's process-global allocation-statistics mutex.
use std::os::raw::c_int;
use std::time::Duration;

use criterion::{criterion_group, criterion_main, Criterion};
use sqlx::sqlite::SqliteConnection;
use sqlx::{Connection, Executor};

const ROWS: i64 = 10_000;

type Numeric = (i64, i64, i64, i64, i64, f64, f64, f64, f64, f64);
type Mixed = (i64, i64, i64, i64, i64, i64, f64, f64, String, String);
type WideText = (i64, i64, f64, String);

const SCHEMA: &str = r#"
CREATE TABLE numeric (
    i0 INTEGER NOT NULL, i1 INTEGER NOT NULL, i2 INTEGER NOT NULL, i3 INTEGER NOT NULL, i4 INTEGER NOT NULL,
    r0 REAL NOT NULL, r1 REAL NOT NULL, r2 REAL NOT NULL, r3 REAL NOT NULL, r4 REAL NOT NULL
);
CREATE TABLE mixed (
    i0 INTEGER NOT NULL, i1 INTEGER NOT NULL, i2 INTEGER NOT NULL,
    i3 INTEGER NOT NULL, i4 INTEGER NOT NULL, i5 INTEGER NOT NULL,
    r0 REAL NOT NULL, r1 REAL NOT NULL,
    t0 TEXT NOT NULL, t1 TEXT NOT NULL
);
CREATE TABLE wide_text (
    id INTEGER NOT NULL, i0 INTEGER NOT NULL, r0 REAL NOT NULL, body TEXT NOT NULL
);
"#;

/// Deterministic data: the same rows on every run and every machine.
const FILL: &str = r#"
WITH RECURSIVE n(x) AS (SELECT 1 UNION ALL SELECT x + 1 FROM n WHERE x < ?1)
INSERT INTO numeric
SELECT x, x * 7919, x * 104729 % 1000003, -x, x * x,
       x * 0.5, x / 3.0, x * 1.000001, -x * 0.25, x * 3.141592653589793
FROM n;

WITH RECURSIVE n(x) AS (SELECT 1 UNION ALL SELECT x + 1 FROM n WHERE x < ?1)
INSERT INTO mixed
SELECT x, x * 7919, x * 104729 % 1000003, -x, x * x, x % 97,
       x * 0.5, x / 3.0,
       printf('user-%015d', x), printf('feed-%015d', x * 31)
FROM n;

WITH RECURSIVE n(x) AS (SELECT 1 UNION ALL SELECT x + 1 FROM n WHERE x < ?1)
INSERT INTO wide_text
SELECT x, x * 7919, x * 0.5,
       substr(replace(hex(zeroblob(500)), '00', printf('%02d', x % 100)), 1, 1000)
FROM n;
"#;

fn configure_sqlite() {
    let memstatus = std::env::var("SQLX_BENCH_SQLITE_MEMSTATUS").unwrap_or_else(|_| "1".into());
    if memstatus == "0" {
        // SAFETY: called before any connection exists, so before `sqlite3_initialize`.
        let rc = unsafe {
            libsqlite3_sys::sqlite3_config(libsqlite3_sys::SQLITE_CONFIG_MEMSTATUS, 0 as c_int)
        };
        assert_eq!(
            rc,
            libsqlite3_sys::SQLITE_OK,
            "sqlite3_config(MEMSTATUS, 0)"
        );
    }
    // SAFETY: returns a static NUL-terminated string.
    let version = unsafe { std::ffi::CStr::from_ptr(libsqlite3_sys::sqlite3_libversion()) };
    eprintln!("sqlite {} memstatus={memstatus}", version.to_string_lossy());
}

async fn setup() -> SqliteConnection {
    let mut conn = SqliteConnection::connect("sqlite::memory:").await.unwrap();
    conn.execute(SCHEMA).await.unwrap();
    for stmt in FILL.split(";\n\n") {
        sqlx::query(stmt)
            .bind(ROWS)
            .execute(&mut conn)
            .await
            .unwrap();
    }
    conn
}

fn fetch_all(c: &mut Criterion) {
    configure_sqlite();
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let conn = std::cell::RefCell::new(runtime.block_on(setup()));

    let mut group = c.benchmark_group("sqlite_fetch_all");
    group.sample_size(30);

    group.bench_function("numeric", |b| {
        b.to_async(&runtime).iter(|| async {
            let rows: Vec<Numeric> = sqlx::query_as("SELECT * FROM numeric")
                .fetch_all(&mut *conn.borrow_mut())
                .await
                .unwrap();
            assert_eq!(rows.len(), ROWS as usize);
            rows
        });
    });

    group.bench_function("mixed", |b| {
        b.to_async(&runtime).iter(|| async {
            let rows: Vec<Mixed> = sqlx::query_as("SELECT * FROM mixed")
                .fetch_all(&mut *conn.borrow_mut())
                .await
                .unwrap();
            assert_eq!(rows.len(), ROWS as usize);
            rows
        });
    });

    group.bench_function("wide_text", |b| {
        b.to_async(&runtime).iter(|| async {
            let rows: Vec<WideText> = sqlx::query_as("SELECT * FROM wide_text")
                .fetch_all(&mut *conn.borrow_mut())
                .await
                .unwrap();
            assert_eq!(rows.len(), ROWS as usize);
            rows
        });
    });

    group.finish();
}

criterion_group! {
    name = benches;
    config = Criterion::default()
        .warm_up_time(Duration::from_secs(2))
        .measurement_time(Duration::from_secs(5));
    targets = fetch_all
}
criterion_main!(benches);
