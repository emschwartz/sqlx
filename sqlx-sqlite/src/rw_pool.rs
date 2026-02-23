use crate::options::{SqliteJournalMode, SqliteSynchronous};
use crate::{Sqlite, SqliteConnectOptions, SqliteQueryResult, SqliteRow, SqliteStatement, SqliteTypeInfo};

use sqlx_core::acquire::Acquire;
use sqlx_core::error::{BoxDynError, Error};
use sqlx_core::executor::{Execute, Executor};
use sqlx_core::pool::{MaybePoolConnection, Pool, PoolConnection, PoolOptions};
use sqlx_core::sql_str::SqlStr;
use sqlx_core::transaction::Transaction;
use sqlx_core::Either;

use futures_core::future::BoxFuture;
use futures_core::stream::BoxStream;
use futures_util::TryStreamExt;

use std::fmt;

// ─── SQL Classification ────────────────────────────────────────────────────────

/// Split `sql` into "words" (maximal runs of alphanumeric/underscore characters).
fn sql_words(sql: &str) -> impl Iterator<Item = &str> {
    sql.split(|c: char| !c.is_ascii_alphanumeric() && c != '_')
        .filter(|w| !w.is_empty())
}

/// Check if any word in `sql` matches `keyword` (case-insensitive).
fn has_keyword(sql: &str, keyword: &str) -> bool {
    sql_words(sql).any(|w| w.eq_ignore_ascii_case(keyword))
}

/// Determines whether a SQL statement should be routed to the read pool.
///
/// Uses conservative heuristics — only definitively read-only statements
/// return `true`. Everything ambiguous routes to the writer (safe default).
///
/// ## Routing rules
///
/// - `SELECT` / `EXPLAIN` → always reader (cannot write in SQLite)
/// - `PRAGMA` without `=` → reader (read-only PRAGMA)
/// - `WITH` CTE → reader **only if** no write keywords appear anywhere
/// - Everything else → writer
#[inline]
pub(crate) fn is_read_only_sql(sql: &str) -> bool {
    let first = match sql_words(sql).next() {
        Some(w) => w,
        None => return false,
    };

    if first.eq_ignore_ascii_case("SELECT") || first.eq_ignore_ascii_case("EXPLAIN") {
        return true;
    }

    if first.eq_ignore_ascii_case("PRAGMA") && !sql.contains('=') {
        return true;
    }

    // WITH CTEs: read-only if no write keywords appear anywhere.
    // Safe false negatives: write keywords in string literals → routes to writer.
    if first.eq_ignore_ascii_case("WITH") {
        return !has_keyword(sql, "INSERT")
            && !has_keyword(sql, "UPDATE")
            && !has_keyword(sql, "DELETE")
            && !has_keyword(sql, "REPLACE");
    }

    false
}

// ─── SqliteRwPoolOptions ───────────────────────────────────────────────────────

/// Builder for [`SqliteRwPool`].
///
/// Provides full control over both the reader and writer pools, including
/// independent [`SqliteConnectOptions`] and [`PoolOptions`] for each.
///
/// # Example
///
/// ```rust,no_run
/// # async fn example() -> sqlx::Result<()> {
/// use sqlx::sqlite::{SqliteRwPoolOptions, SqliteConnectOptions};
/// use sqlx::pool::PoolOptions;
/// use std::time::Duration;
///
/// let pool = SqliteRwPoolOptions::new()
///     .max_readers(4)
///     .writer_pool_options(
///         PoolOptions::new().acquire_timeout(Duration::from_secs(10))
///     )
///     .connect("sqlite://data.db").await?;
/// # Ok(())
/// # }
/// ```
pub struct SqliteRwPoolOptions {
    max_readers: Option<u32>,
    reader_connect_options: Option<SqliteConnectOptions>,
    writer_connect_options: Option<SqliteConnectOptions>,
    reader_pool_options: Option<PoolOptions<Sqlite>>,
    writer_pool_options: Option<PoolOptions<Sqlite>>,
    auto_route: bool,
    checkpoint_on_close: bool,
}

impl Default for SqliteRwPoolOptions {
    fn default() -> Self {
        Self::new()
    }
}

impl SqliteRwPoolOptions {
    /// Create a new `SqliteRwPoolOptions` with sensible defaults.
    ///
    /// Defaults:
    /// - `max_readers`: number of available CPUs (or 4 if unavailable)
    /// - `auto_route`: `true`
    /// - `checkpoint_on_close`: `true`
    pub fn new() -> Self {
        Self {
            max_readers: None,
            reader_connect_options: None,
            writer_connect_options: None,
            reader_pool_options: None,
            writer_pool_options: None,
            auto_route: true,
            checkpoint_on_close: true,
        }
    }

    /// Set the maximum number of reader connections.
    ///
    /// Defaults to the number of available CPUs.
    pub fn max_readers(mut self, max: u32) -> Self {
        self.max_readers = Some(max);
        self
    }

    /// Override the [`SqliteConnectOptions`] used for reader connections.
    ///
    /// WAL journal mode and `read_only(true)` will still be applied on top.
    pub fn reader_connect_options(mut self, opts: SqliteConnectOptions) -> Self {
        self.reader_connect_options = Some(opts);
        self
    }

    /// Override the [`SqliteConnectOptions`] used for the writer connection.
    ///
    /// WAL journal mode and `synchronous(Normal)` will still be applied on top.
    pub fn writer_connect_options(mut self, opts: SqliteConnectOptions) -> Self {
        self.writer_connect_options = Some(opts);
        self
    }

    /// Override the [`PoolOptions`] used for the reader pool.
    ///
    /// `max_connections` will be overridden by [`max_readers`](Self::max_readers)
    /// if also set.
    pub fn reader_pool_options(mut self, opts: PoolOptions<Sqlite>) -> Self {
        self.reader_pool_options = Some(opts);
        self
    }

    /// Override the [`PoolOptions`] used for the writer pool.
    ///
    /// `max_connections` is always forced to 1 for the writer pool.
    pub fn writer_pool_options(mut self, opts: PoolOptions<Sqlite>) -> Self {
        self.writer_pool_options = Some(opts);
        self
    }

    /// Enable or disable automatic SQL-based routing.
    ///
    /// When enabled (the default), the [`Executor`] impl inspects each query's SQL
    /// to decide whether to use the reader or writer pool. When disabled, all
    /// queries go to the writer; readers are only used via [`SqliteRwPool::reader()`].
    pub fn auto_route(mut self, auto_route: bool) -> Self {
        self.auto_route = auto_route;
        self
    }

    /// Run `PRAGMA wal_checkpoint(PASSIVE)` on close.
    ///
    /// Enabled by default. This flushes as much WAL data as possible to the
    /// main database file without blocking.
    pub fn checkpoint_on_close(mut self, checkpoint: bool) -> Self {
        self.checkpoint_on_close = checkpoint;
        self
    }

    /// Create the pool by parsing a connection URL.
    pub async fn connect(self, url: &str) -> Result<SqliteRwPool, Error> {
        let options: SqliteConnectOptions = url.parse()?;
        self.connect_with(options).await
    }

    /// Create the pool from explicit [`SqliteConnectOptions`].
    ///
    /// The writer pool is created first to ensure WAL mode is established
    /// before any readers connect.
    pub async fn connect_with(
        self,
        base_options: SqliteConnectOptions,
    ) -> Result<SqliteRwPool, Error> {
        let num_cpus = std::thread::available_parallelism()
            .map(|n| n.get() as u32)
            .unwrap_or(4);

        // Configure writer: WAL mode + synchronous(Normal)
        let writer_opts = self
            .writer_connect_options
            .unwrap_or_else(|| base_options.clone())
            .journal_mode(SqliteJournalMode::Wal)
            .synchronous(SqliteSynchronous::Normal);

        // Configure reader: read_only only.
        // WAL mode is NOT set here because the reader connection is opened with
        // SQLITE_OPEN_READONLY, and `PRAGMA journal_mode = wal` is a write operation
        // that would deadlock on a read-only connection. The writer already ensures
        // WAL mode is active on the database file; readers inherit it automatically.
        let reader_opts = self
            .reader_connect_options
            .unwrap_or_else(|| base_options)
            .read_only(true);

        // Writer pool: always exactly 1 connection
        let writer_pool_opts = self
            .writer_pool_options
            .unwrap_or_else(PoolOptions::new)
            .max_connections(1);

        // Reader pool: configurable, defaults to num_cpus
        let max_readers = self.max_readers.unwrap_or(num_cpus);
        let reader_pool_opts = self
            .reader_pool_options
            .unwrap_or_else(PoolOptions::new)
            .max_connections(max_readers);

        // Create writer pool FIRST — establishes WAL mode on the database file
        let write_pool = writer_pool_opts.connect_with(writer_opts).await?;

        // Then create reader pool
        let read_pool = reader_pool_opts.connect_with(reader_opts).await?;

        Ok(SqliteRwPool {
            read_pool,
            write_pool,
            auto_route: self.auto_route,
            checkpoint_on_close: self.checkpoint_on_close,
        })
    }
}

// ─── SqliteRwPool ──────────────────────────────────────────────────────────────

/// A single-writer, multi-reader connection pool for SQLite.
///
/// SQLite only allows one writer at a time. When multiple connections compete
/// for the write lock, you get busy timeouts and performance degradation.
/// `SqliteRwPool` solves this by maintaining:
///
/// - A **writer pool** with a single connection for all write operations
/// - A **reader pool** with multiple read-only connections for queries
///
/// # Auto-Routing
///
/// When enabled (the default), the [`Executor`] impl inspects each query's SQL
/// and routes it to the appropriate pool:
///
/// - `SELECT`, `EXPLAIN`, read-only `PRAGMA` → reader pool
/// - `WITH` CTEs without write keywords → reader pool
/// - Everything else → writer pool
///
/// # WAL Mode
///
/// This pool requires and automatically configures
/// [WAL mode](https://www.sqlite.org/wal.html), which allows concurrent
/// readers alongside a single writer.
///
/// # Important
///
/// You must call [`close()`](SqliteRwPool::close) explicitly for the WAL
/// checkpoint to run. Dropping the pool without calling `close()` will skip
/// the checkpoint, even though `checkpoint_on_close` is enabled by default.
/// The checkpoint uses `PASSIVE` mode, which flushes as much WAL data as
/// possible without blocking.
///
/// # Example
///
/// ```rust,no_run
/// # async fn example() -> sqlx::Result<()> {
/// use sqlx::sqlite::SqliteRwPool;
///
/// let pool = SqliteRwPool::connect("sqlite://data.db").await?;
///
/// // SELECT → automatically routed to reader pool
/// let rows = sqlx::query("SELECT * FROM users")
///     .fetch_all(&pool).await?;
///
/// // INSERT → automatically routed to writer pool
/// sqlx::query("INSERT INTO users (name) VALUES (?)")
///     .bind("Alice")
///     .execute(&pool).await?;
///
/// pool.close().await;
/// # Ok(())
/// # }
/// ```
#[derive(Clone)]
pub struct SqliteRwPool {
    read_pool: Pool<Sqlite>,
    write_pool: Pool<Sqlite>,
    auto_route: bool,
    checkpoint_on_close: bool,
}

impl SqliteRwPool {
    /// Create a pool with default options by parsing a connection URL.
    ///
    /// Equivalent to `SqliteRwPoolOptions::new().connect(url)`.
    pub async fn connect(url: &str) -> Result<Self, Error> {
        SqliteRwPoolOptions::new().connect(url).await
    }

    /// Create a pool with default options from explicit connect options.
    ///
    /// Equivalent to `SqliteRwPoolOptions::new().connect_with(options)`.
    pub async fn connect_with(options: SqliteConnectOptions) -> Result<Self, Error> {
        SqliteRwPoolOptions::new().connect_with(options).await
    }

    /// Get a reference to the underlying reader pool.
    ///
    /// Useful for explicitly routing queries to readers, bypassing auto-routing.
    ///
    /// # Note
    ///
    /// Attempting to execute a write statement on a reader connection will
    /// return a `SQLITE_READONLY` error from SQLite.
    pub fn reader(&self) -> &Pool<Sqlite> {
        &self.read_pool
    }

    /// Get a reference to the underlying writer pool.
    pub fn writer(&self) -> &Pool<Sqlite> {
        &self.write_pool
    }

    /// Acquire a read-only connection from the reader pool.
    pub fn acquire_reader(
        &self,
    ) -> impl std::future::Future<Output = Result<PoolConnection<Sqlite>, Error>> + 'static {
        self.read_pool.acquire()
    }

    /// Acquire a writable connection from the writer pool.
    pub fn acquire_writer(
        &self,
    ) -> impl std::future::Future<Output = Result<PoolConnection<Sqlite>, Error>> + 'static {
        self.write_pool.acquire()
    }

    /// Start a transaction on the writer pool.
    pub async fn begin(&self) -> Result<Transaction<'static, Sqlite>, Error> {
        let conn = self.write_pool.acquire().await?;
        Transaction::begin(MaybePoolConnection::PoolConnection(conn), None).await
    }

    /// Start a transaction on the writer pool with a custom `BEGIN` statement.
    pub async fn begin_with(
        &self,
        statement: impl sqlx_core::sql_str::SqlSafeStr,
    ) -> Result<Transaction<'static, Sqlite>, Error> {
        let conn = self.write_pool.acquire().await?;
        Transaction::begin(
            MaybePoolConnection::PoolConnection(conn),
            Some(statement.into_sql_str()),
        )
        .await
    }

    /// Shut down the pool.
    ///
    /// If `checkpoint_on_close` is enabled (the default), closes all reader
    /// connections first, then runs `PRAGMA wal_checkpoint(PASSIVE)` on the
    /// writer to flush as much WAL data as possible to the main database file.
    pub async fn close(&self) {
        // Close readers first so the checkpoint isn't blocked by active readers.
        self.read_pool.close().await;

        if self.checkpoint_on_close && !self.write_pool.is_closed() {
            if let Ok(mut conn) = self.write_pool.acquire().await {
                // Best-effort WAL checkpoint
                let _ = Executor::execute(
                    &mut *conn,
                    "PRAGMA wal_checkpoint(PASSIVE)",
                )
                .await;
            }
        }

        self.write_pool.close().await;
    }

    /// Returns `true` if either pool has been closed.
    pub fn is_closed(&self) -> bool {
        self.write_pool.is_closed() || self.read_pool.is_closed()
    }

    /// Returns the number of active reader connections (including idle).
    pub fn num_readers(&self) -> u32 {
        self.read_pool.size()
    }

    /// Returns the number of idle reader connections.
    pub fn num_idle_readers(&self) -> usize {
        self.read_pool.num_idle()
    }

    /// Returns the number of active writer connections (including idle).
    pub fn num_writers(&self) -> u32 {
        self.write_pool.size()
    }

    /// Returns the number of idle writer connections.
    pub fn num_idle_writers(&self) -> usize {
        self.write_pool.num_idle()
    }
}

impl fmt::Debug for SqliteRwPool {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SqliteRwPool")
            .field("read_pool", &self.read_pool)
            .field("write_pool", &self.write_pool)
            .field("auto_route", &self.auto_route)
            .field("checkpoint_on_close", &self.checkpoint_on_close)
            .finish()
    }
}

// ─── Executor impl ─────────────────────────────────────────────────────────────

/// Carries decomposed query parts for re-delegation to a connection's [`Executor`] impl.
///
/// This allows the `SqliteRwPool` to inspect the SQL for routing before delegating
/// execution to the appropriate pool's connection, without coupling to
/// `SqliteConnection` internals.
struct RoutedQuery {
    sql: SqlStr,
    arguments: Option<crate::SqliteArguments>,
    persistent: bool,
}

impl Execute<'_, Sqlite> for RoutedQuery {
    fn sql(self) -> SqlStr {
        self.sql
    }

    fn statement(&self) -> Option<&SqliteStatement> {
        None
    }

    fn take_arguments(
        &mut self,
    ) -> Result<Option<crate::SqliteArguments>, BoxDynError> {
        Ok(self.arguments.take())
    }

    fn persistent(&self) -> bool {
        self.persistent
    }
}

impl<'p> Executor<'p> for &SqliteRwPool {
    type Database = Sqlite;

    fn fetch_many<'e, 'q, E>(
        self,
        mut query: E,
    ) -> BoxStream<'e, Result<Either<SqliteQueryResult, SqliteRow>, Error>>
    where
        'p: 'e,
        E: Execute<'q, Sqlite>,
        'q: 'e,
        E: 'q,
    {
        let pool = self.clone();

        Box::pin(try_stream! {
            let arguments = query.take_arguments().map_err(Error::Encode)?;
            let persistent = query.persistent();
            let sql = query.sql();

            let use_reader = pool.auto_route && is_read_only_sql(sql.as_str());
            let target_pool = if use_reader { &pool.read_pool } else { &pool.write_pool };
            let mut conn = target_pool.acquire().await?;

            let routed = RoutedQuery { sql, arguments, persistent };
            let mut s = conn.fetch_many(routed);

            while let Some(v) = s.try_next().await? {
                r#yield!(v);
            }

            Ok(())
        })
    }

    fn fetch_optional<'e, 'q, E>(
        self,
        mut query: E,
    ) -> BoxFuture<'e, Result<Option<SqliteRow>, Error>>
    where
        'p: 'e,
        E: Execute<'q, Sqlite>,
        'q: 'e,
        E: 'q,
    {
        let pool = self.clone();

        Box::pin(async move {
            let arguments = query.take_arguments().map_err(Error::Encode)?;
            let persistent = query.persistent();
            let sql = query.sql();

            let use_reader = pool.auto_route && is_read_only_sql(sql.as_str());
            let target_pool = if use_reader { &pool.read_pool } else { &pool.write_pool };
            let mut conn = target_pool.acquire().await?;

            let routed = RoutedQuery { sql, arguments, persistent };
            conn.fetch_optional(routed).await
        })
    }

    fn prepare_with<'e>(
        self,
        sql: SqlStr,
        parameters: &'e [SqliteTypeInfo],
    ) -> BoxFuture<'e, Result<SqliteStatement, Error>>
    where
        'p: 'e,
    {
        let pool = self.write_pool.clone();

        Box::pin(async move { pool.acquire().await?.prepare_with(sql, parameters).await })
    }

    #[doc(hidden)]
    #[cfg(feature = "offline")]
    fn describe<'e>(
        self,
        sql: SqlStr,
    ) -> BoxFuture<'e, Result<sqlx_core::describe::Describe<Sqlite>, Error>>
    where
        'p: 'e,
    {
        let pool = self.write_pool.clone();

        Box::pin(async move { pool.acquire().await?.describe(sql).await })
    }
}

// ─── Acquire impl ──────────────────────────────────────────────────────────────

impl<'a> Acquire<'a> for &SqliteRwPool {
    type Database = Sqlite;
    type Connection = PoolConnection<Sqlite>;

    /// Always acquires from the writer pool.
    ///
    /// This is the safe default because code using `acquire()` may need to
    /// write, and [`sqlx::migrate!().run()`] uses `Acquire` internally.
    /// Use [`SqliteRwPool::acquire_reader()`] for explicit read-only access.
    fn acquire(self) -> BoxFuture<'static, Result<Self::Connection, Error>> {
        Box::pin(self.write_pool.acquire())
    }

    /// Begins a transaction on the writer pool.
    fn begin(self) -> BoxFuture<'static, Result<Transaction<'a, Sqlite>, Error>> {
        let pool = self.write_pool.clone();

        Box::pin(async move {
            let conn = pool.acquire().await?;
            Transaction::begin(MaybePoolConnection::PoolConnection(conn), None).await
        })
    }
}

// ─── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    // SELECT variants
    #[test]
    fn select_is_read_only() {
        assert!(is_read_only_sql("SELECT * FROM users"));
        assert!(is_read_only_sql("select * from users"));
        assert!(is_read_only_sql("Select 1"));
        assert!(is_read_only_sql("  SELECT 1"));
        assert!(is_read_only_sql("\n\tSELECT 1"));
        assert!(is_read_only_sql("SELECT count(*) FROM orders WHERE status = 'active'"));
    }

    // EXPLAIN
    #[test]
    fn explain_is_read_only() {
        assert!(is_read_only_sql("EXPLAIN SELECT 1"));
        assert!(is_read_only_sql("EXPLAIN QUERY PLAN SELECT * FROM users"));
        assert!(is_read_only_sql("explain query plan select 1"));
    }

    // PRAGMA
    #[test]
    fn pragma_routing() {
        // Read-only PRAGMAs (no =)
        assert!(is_read_only_sql("PRAGMA journal_mode"));
        assert!(is_read_only_sql("PRAGMA table_info(users)"));
        assert!(is_read_only_sql("pragma page_count"));

        // Write PRAGMAs (with =)
        assert!(!is_read_only_sql("PRAGMA journal_mode = WAL"));
        assert!(!is_read_only_sql("PRAGMA synchronous = NORMAL"));
    }

    // WITH CTEs
    #[test]
    fn with_cte_routing() {
        // Read-only CTEs
        assert!(is_read_only_sql(
            "WITH t AS (SELECT 1) SELECT * FROM t"
        ));
        assert!(is_read_only_sql(
            "WITH RECURSIVE cte(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM cte WHERE n < 10) SELECT * FROM cte"
        ));

        // Write CTEs
        assert!(!is_read_only_sql(
            "WITH t AS (SELECT 1) INSERT INTO foo SELECT * FROM t"
        ));
        assert!(!is_read_only_sql(
            "WITH t AS (SELECT 1) UPDATE foo SET bar = 1"
        ));
        assert!(!is_read_only_sql(
            "WITH t AS (SELECT 1) DELETE FROM foo"
        ));
        assert!(!is_read_only_sql(
            "WITH t AS (SELECT 1) REPLACE INTO foo SELECT * FROM t"
        ));
    }

    // Write operations → writer
    #[test]
    fn write_operations_are_not_read_only() {
        assert!(!is_read_only_sql("INSERT INTO users VALUES (1)"));
        assert!(!is_read_only_sql("UPDATE users SET name = 'Bob'"));
        assert!(!is_read_only_sql("DELETE FROM users"));
        assert!(!is_read_only_sql("REPLACE INTO users VALUES (1)"));
        assert!(!is_read_only_sql("CREATE TABLE foo (id INT)"));
        assert!(!is_read_only_sql("DROP TABLE foo"));
        assert!(!is_read_only_sql("ALTER TABLE foo ADD COLUMN bar INT"));
        assert!(!is_read_only_sql("CREATE INDEX idx ON foo(bar)"));
    }

    // Transaction control → writer
    #[test]
    fn transaction_control_is_not_read_only() {
        assert!(!is_read_only_sql("BEGIN"));
        assert!(!is_read_only_sql("BEGIN TRANSACTION"));
        assert!(!is_read_only_sql("COMMIT"));
        assert!(!is_read_only_sql("ROLLBACK"));
        assert!(!is_read_only_sql("SAVEPOINT sp1"));
    }

    // Edge cases
    #[test]
    fn edge_cases() {
        // Empty / whitespace
        assert!(!is_read_only_sql(""));
        assert!(!is_read_only_sql("   "));

        // Keywords as substrings should NOT match
        assert!(!is_read_only_sql("SELECTIVITY_CHECK()"));

        // ATTACH / DETACH → writer
        assert!(!is_read_only_sql("ATTACH DATABASE ':memory:' AS db2"));
        assert!(!is_read_only_sql("DETACH DATABASE db2"));

        // False negative: write keyword in string literal → safely routes to writer
        assert!(!is_read_only_sql(
            "WITH t AS (SELECT 'DELETE') SELECT * FROM t"
        ));
    }

    // Word boundary checks
    #[test]
    fn word_boundary_checks() {
        // "SELECTED" should not match "SELECT"
        assert!(!is_read_only_sql("SELECTED * FROM foo"));

        // "SELECTS" should not match "SELECT"
        assert!(!is_read_only_sql("SELECTS something"));

        // "INSERTS" in a WITH should not trigger the INSERT guard
        // (but it also won't match as a standalone keyword)
        assert!(is_read_only_sql(
            "WITH t AS (SELECT * FROM inserts_log) SELECT * FROM t"
        ));

        // "UPDATE" as a column name in a table won't match because
        // it would need word boundaries
        assert!(is_read_only_sql(
            "WITH t AS (SELECT updated FROM foo) SELECT * FROM t"
        ));
    }
}
