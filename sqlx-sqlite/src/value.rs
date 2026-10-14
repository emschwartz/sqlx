use std::borrow::Cow;
use std::cell::OnceCell;
use std::ptr::NonNull;
use std::slice;
use std::str;

use libsqlite3_sys::{
    sqlite3_value, sqlite3_value_blob, sqlite3_value_bytes, sqlite3_value_double,
    sqlite3_value_int64, sqlite3_value_type, SQLITE_BLOB, SQLITE_FLOAT, SQLITE_INTEGER,
    SQLITE_NULL, SQLITE_TEXT,
};
use sqlx_core::type_info::TypeInfo;
pub(crate) use sqlx_core::value::{Value, ValueRef};

use crate::type_info::DataType;
use crate::{Sqlite, SqliteError, SqliteTypeInfo};

/// An owned handle to a [`sqlite3_value`].
///
/// # Note: Decoding is Stateful
/// The [`sqlite3_value` interface][value-methods] reserves the right to be stateful:
///
/// > Other interfaces might change the datatype for an sqlite3_value object.
/// > For example, if the datatype is initially SQLITE_INTEGER and sqlite3_value_text(V) is called
/// > to extract a text value for that integer, then subsequent calls to sqlite3_value_type(V)
/// > might return SQLITE_TEXT. Whether or not a persistent internal datatype conversion occurs is
/// > undefined and may change from one release of SQLite to the next.
///
/// Thus, this type is `!Sync` and [`SqliteValueRef`] is `!Send` and `!Sync` to prevent data races.
///
/// Additionally, this statefulness means that the return values of `sqlite3_value_bytes()` and
/// `sqlite3_value_blob()` could be invalidated by later calls to other `sqlite3_value*` methods.
///
/// To prevent undefined behavior from accessing dangling pointers, this type (and any
/// [`SqliteValueRef`] instances created from it) remembers when it was used to decode a
/// borrowed `&[u8]` or `&str` and returns an error if it is used to decode any other type.
///
/// To bypass this error, you must prove that no outstanding borrows exist.
///
/// This may be done in one of a few ways:
/// * If you hold mutable access, call [`Self::reset_borrow()`] which resets the borrowed state.
/// * If you have an immutable reference, call [`Self::clone()`] to get a new instance
///   with no outstanding borrows.
/// * If you hold a [`SqliteValueRef`], call [`SqliteValueRef::to_owned()`]
///   to get a new `SqliteValue` with no outstanding borrows.
///
/// This is *only* necessary if using the same `SqliteValue` or [`SqliteValueRef`] to decode
/// multiple different types. The vast majority of use-cases employing once-through decoding
/// should not have to worry about this.
///
/// [`sqlite3_value`]: https://www.sqlite.org/c3ref/value.html
/// [value-methods]: https://www.sqlite.org/c3ref/value_blob.html
pub struct SqliteValue(ValueHandle);

/// A borrowed reference to a [`sqlite3_value`].
///
/// Semantically, this behaves as a reference to [`SqliteValue`].
///
/// # Note: Decoding is Stateful
/// See [`SqliteValue`] for details.
pub struct SqliteValueRef<'r>(Cow<'r, ValueHandle>);

impl SqliteValue {
    // SAFETY: The sqlite3_value must be non-null and SQLite must not free it. It will be freed on drop.
    pub(crate) unsafe fn dup(
        value: *mut sqlite3_value,
        column_type: Option<SqliteTypeInfo>,
    ) -> Self {
        debug_assert!(!value.is_null());
        let handle = ValueHandle::try_dup_of(value, column_type)
            .expect("SQLite failed to allocate memory for duplicated value");
        Self(handle)
    }

    /// Prove that there are no outstanding borrows of this instance.
    ///
    /// Call this after decoding a borrowed `&[u8]` or `&str`
    /// to reset the internal borrowed state and allow decoding of other types.
    pub fn reset_borrow(&mut self) {
        self.0.reset_blob_borrow();
    }

    /// Call [`sqlite3_value_dup()`] to create a new instance of this type.
    ///
    /// Returns an error if the call returns a null pointer, indicating that
    /// SQLite was unable to allocate the additional memory required.
    ///
    /// Non-panicking version of [`Self::clone()`].
    ///
    /// [`sqlite3_value_dup()`]: https://www.sqlite.org/c3ref/value_dup.html
    pub fn try_clone(&self) -> Result<Self, SqliteError> {
        self.0.try_dup().map(Self)
    }
}

impl Clone for SqliteValue {
    /// Call [`sqlite3_value_dup()`] to create a new instance of this type.
    ///
    /// # Panics
    /// If [`sqlite3_value_dup()`] returns a null pointer, indicating an out-of-memory condition.
    ///
    /// See [`Self::try_clone()`] for a non-panicking version.
    ///
    /// [`sqlite3_value_dup()`]: https://www.sqlite.org/c3ref/value_dup.html
    fn clone(&self) -> Self {
        self.try_clone().expect("failed to clone `SqliteValue`")
    }
}

impl Value for SqliteValue {
    type Database = Sqlite;

    fn as_ref(&self) -> SqliteValueRef<'_> {
        SqliteValueRef::value(self)
    }

    fn type_info(&self) -> Cow<'_, SqliteTypeInfo> {
        Cow::Owned(self.0.type_info())
    }

    fn is_null(&self) -> bool {
        self.0.is_null()
    }
}

impl<'r> SqliteValueRef<'r> {
    /// Attempt to duplicate the internal `sqlite3_value` with [`sqlite3_value_dup()`].
    ///
    /// Returns an error if the call returns a null pointer, indicating that
    /// SQLite was unable to allocate the additional memory required.
    ///
    /// Non-panicking version of [`Self::try_to_owned()`].
    ///
    /// [`sqlite3_value_dup()`]: https://www.sqlite.org/c3ref/value_dup.html
    pub fn try_to_owned(&self) -> Result<SqliteValue, SqliteError> {
        self.0.try_dup().map(SqliteValue)
    }

    pub(crate) fn value(value: &'r SqliteValue) -> Self {
        Self(Cow::Borrowed(&value.0))
    }

    /// # Safety
    /// The supplied sqlite3_value must not be null and SQLite must free it.
    /// It will not be freed on drop.
    /// The lifetime on this struct should tie it to whatever scope it's valid for before SQLite frees it.
    #[allow(unused)]
    pub(crate) unsafe fn borrowed(value: *mut sqlite3_value) -> Self {
        debug_assert!(!value.is_null());
        let handle = ValueHandle::temporary(NonNull::new_unchecked(value));
        Self(Cow::Owned(handle))
    }

    // NOTE: `int()` is deliberately omitted because it will silently truncate a wider value,
    // which is likely to cause bugs:
    // https://github.com/launchbadge/sqlx/issues/3179
    // (Similar bug in Postgres): https://github.com/launchbadge/sqlx/issues/3161
    pub(super) fn int64(&self) -> Result<i64, BorrowedBlobError> {
        self.0.int64()
    }

    pub(super) fn double(&self) -> Result<f64, BorrowedBlobError> {
        self.0.double()
    }

    pub(super) fn blob_borrowed(&self) -> &'r [u8] {
        // SAFETY: lifetime is matched to `'r`
        unsafe { self.0.blob_borrowed() }
    }

    pub(super) fn with_temp_blob<R>(&self, op: impl FnOnce(&[u8]) -> R) -> R {
        self.0.with_blob(op)
    }

    pub(super) fn blob_owned(&self) -> Vec<u8> {
        self.with_temp_blob(|blob| blob.to_vec())
    }

    pub(super) fn text_borrowed(&self) -> Result<&'r str, str::Utf8Error> {
        // SAFETY: lifetime is matched to `'r`
        unsafe { self.0.text_borrowed() }
    }

    pub(super) fn with_temp_text<R>(
        &self,
        op: impl FnOnce(&str) -> R,
    ) -> Result<R, str::Utf8Error> {
        self.0.with_blob(|blob| str::from_utf8(blob).map(op))
    }

    pub(super) fn text_owned(&self) -> Result<String, str::Utf8Error> {
        self.with_temp_text(|text| text.to_string())
    }
}

impl<'r> ValueRef<'r> for SqliteValueRef<'r> {
    type Database = Sqlite;

    /// Attempt to duplicate the internal `sqlite3_value` with [`sqlite3_value_dup()`].
    ///
    /// # Panics
    /// If [`sqlite3_value_dup()`] returns a null pointer, indicating an out-of-memory condition.
    ///
    /// See [`Self::try_to_owned()`] for a non-panicking version.
    ///
    /// [`sqlite3_value_dup()`]: https://www.sqlite.org/c3ref/value_dup.html
    fn to_owned(&self) -> SqliteValue {
        SqliteValue(
            self.0
                .try_dup()
                .expect("failed to convert SqliteValueRef to owned SqliteValue"),
        )
    }

    fn type_info(&self) -> Cow<'_, SqliteTypeInfo> {
        Cow::Owned(self.0.type_info())
    }

    fn is_null(&self) -> bool {
        self.0.is_null()
    }
}

pub(crate) struct ValueHandle {
    repr: Repr,
    column_type: Option<SqliteTypeInfo>,
    // Note: `std::cell` version
    borrowed_blob: OnceCell<Blob>,
    /// TEXT rendering of an owned INTEGER or REAL, made on first TEXT/BLOB
    /// access: what `sqlite3_value_text` would have produced.
    rendered: OnceCell<Box<[u8]>>,
}

/// A column value copied out of the statement's row into Rust memory, by
/// storage class. Copying replaces `sqlite3_value_dup`/`sqlite3_value_free`,
/// which cost two `sqlite3_malloc`s and two frees per value, each under
/// SQLite's process-global memstat mutex, contended between the worker
/// thread (dup) and whichever thread drops the row (free).
///
/// Every accessor reproduces what the corresponding `sqlite3_value_*` call
/// returns for that storage class (see `int64`, `double`, `get_blob`).
/// `Raw` remains only for borrowed, never-freed values (the preupdate hook).
enum Repr {
    Null,
    Int(i64),
    Real(f64),
    Text(Box<[u8]>),
    Blob(Box<[u8]>),
    Raw(NonNull<sqlite3_value>),
}

struct Blob {
    ptr: *const u8,
    len: usize,
}

#[derive(Debug, thiserror::Error)]
#[error("given `SqliteValue` was previously decoded as BLOB or TEXT; `SqliteValue::reset_borrow()` must be called first")]
pub(crate) struct BorrowedBlobError;

// SAFE: owned values are plain data; `Raw` values are protected and only
// ever read on the thread that owns the hook callback.
unsafe impl Send for ValueHandle {}

// SAFETY: the `sqlite3_value_*()` methods reserve the right to be stateful,
// which means method calls aren't thread-safe without mutual exclusion.
//
// impl !Sync for ValueHandle {}

/// The bytes `sqlite3_value_blob` returns for a TEXT or BLOB value.
///
/// # Safety
/// `value` must be valid for the duration of the call.
unsafe fn copy_bytes(value: *mut sqlite3_value) -> Box<[u8]> {
    // `sqlite3_value_blob` before `sqlite3_value_bytes`: the order SQLite
    // documents as safe (no conversion happens between the two calls).
    let ptr = unsafe { sqlite3_value_blob(value) } as *const u8;
    let len = unsafe { sqlite3_value_bytes(value) };
    let len = usize::try_from(len).unwrap_or(0);
    if len == 0 || ptr.is_null() {
        return Box::default();
    }
    unsafe { slice::from_raw_parts(ptr, len) }.into()
}

impl ValueHandle {
    /// Copy `value` into Rust memory.
    ///
    /// # Safety
    /// The `sqlite3_value` must be valid for the duration of the call.
    unsafe fn try_dup_of(
        value: *mut sqlite3_value,
        column_type: Option<SqliteTypeInfo>,
    ) -> Result<Self, SqliteError> {
        let repr = match unsafe { sqlite3_value_type(value) } {
            SQLITE_NULL => Repr::Null,
            SQLITE_INTEGER => Repr::Int(unsafe { sqlite3_value_int64(value) }),
            SQLITE_FLOAT => Repr::Real(unsafe { sqlite3_value_double(value) }),
            SQLITE_TEXT => Repr::Text(unsafe { copy_bytes(value) }),
            _ => Repr::Blob(unsafe { copy_bytes(value) }),
        };
        Ok(Self::with_repr(repr, column_type))
    }

    fn with_repr(repr: Repr, column_type: Option<SqliteTypeInfo>) -> Self {
        Self {
            repr,
            column_type,
            borrowed_blob: OnceCell::new(),
            rendered: OnceCell::new(),
        }
    }

    fn temporary(value: NonNull<sqlite3_value>) -> Self {
        Self::with_repr(Repr::Raw(value), None)
    }

    fn try_dup(&self) -> Result<Self, SqliteError> {
        let repr = match &self.repr {
            Repr::Null => Repr::Null,
            Repr::Int(i) => Repr::Int(*i),
            Repr::Real(r) => Repr::Real(*r),
            Repr::Text(b) => Repr::Text(b.clone()),
            Repr::Blob(b) => Repr::Blob(b.clone()),
            // SAFETY: a temporary is valid while its borrower is
            Repr::Raw(value) => {
                return unsafe { Self::try_dup_of(value.as_ptr(), self.column_type.clone()) }
            }
        };
        Ok(Self::with_repr(repr, self.column_type.clone()))
    }

    fn value_type_info(&self) -> SqliteTypeInfo {
        SqliteTypeInfo(DataType::from_code(match &self.repr {
            Repr::Null => SQLITE_NULL,
            Repr::Int(_) => SQLITE_INTEGER,
            Repr::Real(_) => SQLITE_FLOAT,
            Repr::Text(_) => SQLITE_TEXT,
            Repr::Blob(_) => SQLITE_BLOB,
            Repr::Raw(value) => unsafe { sqlite3_value_type(value.as_ptr()) },
        }))
    }

    fn type_info(&self) -> SqliteTypeInfo {
        let value_type = self.value_type_info();

        // Assume the actual value type is more accurate, if it's not NULL.
        match &self.column_type {
            Some(column_type) if value_type.is_null() => column_type.clone(),
            _ => value_type,
        }
    }

    fn int64(&self) -> Result<i64, BorrowedBlobError> {
        // SAFETY: we have to be certain the caller isn't still holding a borrow from `.blob_borrowed()`
        self.assert_blob_not_borrowed()?;

        Ok(match &self.repr {
            Repr::Null => 0,
            Repr::Int(i) => *i,
            // `doubleToInt64`: saturating, as Rust's `as` is.
            #[allow(clippy::cast_possible_truncation)]
            Repr::Real(r) => *r as i64,
            Repr::Text(b) | Repr::Blob(b) => coerce::int64(b),
            Repr::Raw(value) => unsafe { sqlite3_value_int64(value.as_ptr()) },
        })
    }

    fn double(&self) -> Result<f64, BorrowedBlobError> {
        // SAFETY: we have to be certain the caller isn't still holding a borrow from `.blob_borrowed()`
        self.assert_blob_not_borrowed()?;

        Ok(match &self.repr {
            Repr::Null => 0.0,
            // `sqlite3VdbeRealValue` on MEM_Int is the same `(double)i` cast.
            #[allow(clippy::cast_precision_loss)]
            Repr::Int(i) => *i as f64,
            Repr::Real(r) => *r,
            Repr::Text(b) | Repr::Blob(b) => coerce::double(b),
            Repr::Raw(value) => unsafe { sqlite3_value_double(value.as_ptr()) },
        })
    }

    fn is_null(&self) -> bool {
        self.value_type_info().is_null()
    }
}

impl Clone for ValueHandle {
    fn clone(&self) -> Self {
        self.try_dup().unwrap()
    }
}

impl ValueHandle {
    fn assert_blob_not_borrowed(&self) -> Result<(), BorrowedBlobError> {
        if self.borrowed_blob.get().is_none() {
            Ok(())
        } else {
            Err(BorrowedBlobError)
        }
    }

    fn reset_blob_borrow(&mut self) {
        self.borrowed_blob.take();
    }

    fn get_blob(&self) -> Option<Blob> {
        if let Some(blob) = self.borrowed_blob.get() {
            return Some(Blob { ..*blob });
        }

        let bytes: &[u8] = match &self.repr {
            Repr::Null => return None,
            Repr::Int(i) => self
                .rendered
                .get_or_init(|| i.to_string().into_bytes().into_boxed_slice()),
            Repr::Real(r) => self.rendered.get_or_init(|| coerce::render_real(*r)),
            Repr::Text(b) | Repr::Blob(b) => b,
            Repr::Raw(value) => {
                // SAFETY: calling `sqlite3_value_bytes` from multiple threads at once is a data race.
                let len = unsafe { sqlite3_value_bytes(value.as_ptr()) };
                let len = usize::try_from(len).unwrap_or_else(|_| {
                    panic!("sqlite3_value_bytes() returned value out of range for usize: {len}")
                });
                if len == 0 {
                    return None;
                }
                let ptr = unsafe { sqlite3_value_blob(value.as_ptr()) } as *const u8;
                debug_assert!(!ptr.is_null());
                return Some(Blob { ptr, len });
            }
        };

        if bytes.is_empty() {
            // empty blobs are NULL
            return None;
        }

        Some(Blob {
            ptr: bytes.as_ptr(),
            len: bytes.len(),
        })
    }

    fn with_blob<R>(&self, with_blob: impl FnOnce(&[u8]) -> R) -> R {
        let Some(blob) = self.get_blob() else {
            return with_blob(&[]);
        };

        // SAFETY: the slice cannot outlive the call
        with_blob(unsafe { blob.as_slice() })
    }

    /// # Safety
    /// Caller must ensure lifetime '`b` cannot outlive `self`.
    unsafe fn blob_borrowed<'a>(&self) -> &'a [u8] {
        let Some(blob) = self.get_blob() else {
            return &[];
        };

        let blob = self.borrowed_blob.get_or_init(|| blob);

        unsafe { blob.as_slice() }
    }

    /// # Safety
    /// Caller must ensure lifetime '`b` cannot outlive `self`.
    unsafe fn text_borrowed<'b>(&self) -> Result<&'b str, str::Utf8Error> {
        let Some(blob) = self.get_blob() else {
            return Ok("");
        };

        // SAFETY: lifetime of `blob` will be tied to `'b`.
        let s = str::from_utf8(unsafe { blob.as_slice() })?;

        // We only store the borrow after we ensure the string is valid.
        self.borrowed_blob.set(blob).ok();

        Ok(s)
    }
}

/// TEXT/BLOB -> number and REAL -> TEXT coercions for owned values, which
/// reproduce what `sqlite3_value_int64`, `sqlite3_value_double` and
/// `sqlite3_value_text` return for those storage classes.
///
/// These run only for cross-class reads (`try_get_unchecked`, the `query!`
/// macros); a value read as its own storage class never comes here.
mod coerce {
    use std::ffi::{c_char, c_int, CStr};

    /// `sqlite3Isspace`: ASCII whitespace including vertical tab.
    fn is_space(b: u8) -> bool {
        matches!(b, b' ' | b'\t' | b'\n' | 0x0b | b'\x0c' | b'\r')
    }

    fn skip_digits(bytes: &[u8], mut i: usize) -> usize {
        while bytes.get(i).is_some_and(u8::is_ascii_digit) {
            i += 1;
        }
        i
    }

    /// The longest prefix a numeric parse consumes, in one linear pass:
    /// `(start, int_end, float_end, has_digits)`. `start` is the optional sign
    /// after leading whitespace; `int_end` ends the integer digits and
    /// `float_end` the fraction and exponent. All bound ASCII bytes, so
    /// slicing there never splits a UTF-8 character.
    fn scan(bytes: &[u8]) -> (usize, usize, usize, bool) {
        let start = bytes
            .iter()
            .position(|&b| !is_space(b))
            .unwrap_or(bytes.len());
        let mut i = start;
        if matches!(bytes.get(i), Some(b'-' | b'+')) {
            i += 1;
        }
        let int_end = skip_digits(bytes, i);
        let mut has_digits = int_end > i;
        let mut end = int_end;
        if bytes.get(end) == Some(&b'.') {
            let frac_end = skip_digits(bytes, end + 1);
            has_digits |= frac_end > end + 1;
            end = frac_end;
        }
        if has_digits && matches!(bytes.get(end), Some(b'e' | b'E')) {
            let mut j = end + 1;
            if matches!(bytes.get(j), Some(b'-' | b'+')) {
                j += 1;
            }
            let exp_end = skip_digits(bytes, j);
            // an exponent marker with no digits is not part of the number
            if exp_end > j {
                end = exp_end;
            }
        }
        (start, int_end, end, has_digits)
    }

    fn ascii(bytes: &[u8]) -> &str {
        // `scan` only ever bounds ASCII sign, digit, '.' and exponent bytes.
        std::str::from_utf8(bytes).expect("numeric prefix is ASCII")
    }

    /// `sqlite3_value_int64` of a TEXT or BLOB value (`sqlite3Atoi64`): the
    /// integer prefix, saturating at the `i64` bounds; 0 if there is none.
    pub(super) fn int64(bytes: &[u8]) -> i64 {
        let (start, int_end, _, _) = scan(bytes);
        let digits = ascii(&bytes[start..int_end]);
        let negative = digits.starts_with('-');
        if !digits.bytes().any(|b| b.is_ascii_digit()) {
            return 0;
        }
        digits
            .parse::<i64>()
            .unwrap_or(if negative { i64::MIN } else { i64::MAX })
    }

    /// `sqlite3_value_double` of a TEXT or BLOB value (`sqlite3AtoF`): the
    /// longest decimal prefix, correctly rounded. SQLite's own parser can
    /// differ in the last bit on long or extreme inputs; see the tests.
    pub(super) fn double(bytes: &[u8]) -> f64 {
        let (start, _, end, has_digits) = scan(bytes);
        if !has_digits {
            // SQLite keeps the sign of a bare "-": the result is -0.0
            return if bytes.get(start) == Some(&b'-') {
                -0.0
            } else {
                0.0
            };
        }
        ascii(&bytes[start..end]).parse::<f64>().unwrap_or(0.0)
    }

    /// `vdbeMemRenderNum` for a REAL: SQLite's own printf, `"%!.15g"`.
    pub(super) fn render_real(r: f64) -> Box<[u8]> {
        const LEN: c_int = 32;
        let mut buf = [0 as c_char; LEN as usize];
        // SAFETY: SQLite writes at most `LEN` bytes, including the NUL.
        unsafe {
            libsqlite3_sys::sqlite3_snprintf(LEN, buf.as_mut_ptr(), c"%!.15g".as_ptr(), r);
            CStr::from_ptr(buf.as_ptr()).to_bytes().into()
        }
    }
}

impl Blob {
    /// # Safety
    /// `'a` must not outlive the `sqlite3_value` this blob came from.
    unsafe fn as_slice<'a>(&self) -> &'a [u8] {
        slice::from_raw_parts(self.ptr, self.len)
    }
}

/// Every accessor of an owned value against SQLite reading the same value
/// in place: `int64`, `double` and the TEXT/BLOB bytes, for each storage class.
#[cfg(test)]
#[allow(
    clippy::cast_possible_truncation,
    clippy::cast_possible_wrap,
    clippy::cast_precision_loss,
    clippy::cast_sign_loss
)]
mod oracle_tests {
    use super::ValueHandle;
    use libsqlite3_sys as ffi;
    use std::ptr;

    #[derive(Clone, Copy)]
    pub(super) enum Input<'a> {
        Int(i64),
        Real(f64),
        Text(&'a [u8]),
        Blob(&'a [u8]),
    }

    #[derive(Debug, PartialEq)]
    pub(super) struct Reads {
        pub int: i64,
        pub real: u64,
        pub bytes: Vec<u8>,
    }

    struct Stmt {
        db: *mut ffi::sqlite3,
        stmt: *mut ffi::sqlite3_stmt,
    }

    impl Stmt {
        fn new() -> Self {
            unsafe {
                let mut db = ptr::null_mut();
                assert_eq!(
                    ffi::sqlite3_open(c":memory:".as_ptr(), &mut db),
                    ffi::SQLITE_OK
                );
                let mut stmt = ptr::null_mut();
                assert_eq!(
                    ffi::sqlite3_prepare_v2(
                        db,
                        c"SELECT ?".as_ptr(),
                        -1,
                        &mut stmt,
                        ptr::null_mut()
                    ),
                    ffi::SQLITE_OK
                );
                Self { db, stmt }
            }
        }

        /// Bind `input`, step to its row, run `read`, reset.
        fn with_row<R>(
            &self,
            input: Input<'_>,
            read: impl FnOnce(*mut ffi::sqlite3_stmt) -> R,
        ) -> R {
            unsafe {
                let rc = match input {
                    Input::Int(i) => ffi::sqlite3_bind_int64(self.stmt, 1, i),
                    Input::Real(r) => ffi::sqlite3_bind_double(self.stmt, 1, r),
                    Input::Text(t) => ffi::sqlite3_bind_text(
                        self.stmt,
                        1,
                        t.as_ptr().cast(),
                        i32::try_from(t.len()).unwrap(),
                        ffi::SQLITE_TRANSIENT(),
                    ),
                    Input::Blob(b) => ffi::sqlite3_bind_blob(
                        self.stmt,
                        1,
                        b.as_ptr().cast(),
                        i32::try_from(b.len()).unwrap(),
                        ffi::SQLITE_TRANSIENT(),
                    ),
                };
                assert_eq!(rc, ffi::SQLITE_OK);
                assert_eq!(ffi::sqlite3_step(self.stmt), ffi::SQLITE_ROW);
                let out = read(self.stmt);
                ffi::sqlite3_reset(self.stmt);
                ffi::sqlite3_clear_bindings(self.stmt);
                out
            }
        }

        /// SQLite's answers, each on a fresh step: a read may convert the value in place.
        fn sqlite(&self, input: Input<'_>) -> Reads {
            unsafe {
                Reads {
                    int: self.with_row(input, |s| ffi::sqlite3_column_int64(s, 0)),
                    real: self.with_row(input, |s| ffi::sqlite3_column_double(s, 0).to_bits()),
                    bytes: self.with_row(input, |s| {
                        let p = ffi::sqlite3_column_blob(s, 0) as *const u8;
                        let n = usize::try_from(ffi::sqlite3_column_bytes(s, 0)).unwrap();
                        if n == 0 {
                            Vec::new()
                        } else {
                            std::slice::from_raw_parts(p, n).to_vec()
                        }
                    }),
                }
            }
        }

        /// The owned value's answers, each from a value copied out of a
        /// fresh step and read after the statement was reset.
        fn owned(&self, input: Input<'_>) -> Reads {
            let handle = || {
                self.with_row(input, |s| unsafe {
                    ValueHandle::try_dup_of(ffi::sqlite3_column_value(s, 0), None).unwrap()
                })
            };
            Reads {
                int: handle().int64().unwrap(),
                real: handle().double().unwrap().to_bits(),
                bytes: handle().with_blob(<[u8]>::to_vec),
            }
        }
    }

    impl Drop for Stmt {
        fn drop(&mut self) {
            unsafe {
                ffi::sqlite3_finalize(self.stmt);
                ffi::sqlite3_close(self.db);
            }
        }
    }

    /// Splitmix64: a fixed, dependency-free input stream.
    pub(super) struct Rng(u64);

    impl Rng {
        pub(super) fn new(seed: u64) -> Self {
            Self(seed)
        }

        pub(super) fn next(&mut self) -> u64 {
            self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
            let mut z = self.0;
            z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
            z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
            z ^ (z >> 31)
        }

        fn below(&mut self, n: u64) -> u64 {
            self.next() % n
        }

        /// A numeric-looking string: optional whitespace and sign, digits,
        /// optional fraction and exponent, sometimes trailing garbage.
        pub(super) fn numeric_text(&mut self) -> String {
            let mut s = String::new();
            for _ in 0..self.below(3) {
                s.push([' ', '\t', '\n'][self.below(3) as usize]);
            }
            match self.below(4) {
                0 => s.push('-'),
                1 => s.push('+'),
                _ => {}
            }
            let digits = |rng: &mut Self, s: &mut String, max: u64| {
                for _ in 0..rng.below(max) {
                    s.push(char::from(b'0' + rng.below(10) as u8));
                }
            };
            digits(self, &mut s, 25);
            if self.below(2) == 0 {
                s.push('.');
                digits(self, &mut s, 25);
            }
            if self.below(3) == 0 {
                s.push(['e', 'E'][self.below(2) as usize]);
                match self.below(3) {
                    0 => s.push('-'),
                    1 => s.push('+'),
                    _ => {}
                }
                digits(self, &mut s, 4);
            }
            if self.below(5) == 0 {
                s.push_str(["x", " ", "\u{e9}", ".5", "e"][self.below(5) as usize]);
            }
            s
        }
    }

    /// Owned reads must equal SQLite's exactly, except that `allow_real_ulp`
    /// tolerates up to `MAX_ULPS` difference in `double` of TEXT/BLOB values,
    /// where SQLite's `sqlite3AtoF` is not correctly rounded.
    /// Returns the inputs whose `double` differed.
    pub(super) const MAX_ULPS: u64 = 4;

    pub(super) fn compare(inputs: &[Input<'_>], allow_real_ulp: bool) -> Vec<(String, f64, f64)> {
        let stmt = Stmt::new();
        let mut diffs = Vec::new();
        for &input in inputs {
            let (want, got) = (stmt.sqlite(input), stmt.owned(input));
            let shown = match input {
                Input::Int(i) => format!("INTEGER {i}"),
                Input::Real(r) => format!("REAL {r:e}"),
                Input::Text(t) => {
                    format!(
                        "TEXT {:?}",
                        String::from_utf8_lossy(&t[..std::cmp::min(t.len(), 40)])
                    )
                }
                Input::Blob(b) => format!("BLOB {:?}", &b[..std::cmp::min(b.len(), 40)]),
            };
            assert_eq!(got.int, want.int, "int64 of {shown}");
            assert_eq!(got.bytes, want.bytes, "bytes of {shown}");
            if got.real != want.real {
                let cross = matches!(input, Input::Text(_) | Input::Blob(_));
                let ulps = got.real.abs_diff(want.real);
                assert!(
                    allow_real_ulp && cross && ulps <= MAX_ULPS,
                    "double of {shown}: ours {:e} sqlite {:e} ({ulps} ulp)",
                    f64::from_bits(got.real),
                    f64::from_bits(want.real),
                );
                diffs.push((shown, f64::from_bits(got.real), f64::from_bits(want.real)));
            }
        }
        diffs
    }

    pub(super) fn every_class(rng: &mut Rng, n: usize) -> (Vec<String>, Vec<i64>, Vec<f64>) {
        let texts: Vec<String> = (0..n).map(|_| rng.numeric_text()).collect();
        let mut ints = vec![0, 1, -1, i64::MAX, i64::MIN];
        let mut reals = vec![
            0.0,
            -0.0,
            0.1,
            1e15,
            1e16,
            1e300,
            -1e-300,
            5e-324,
            f64::MAX,
            f64::MIN_POSITIVE,
        ];
        reals.extend([
            f64::INFINITY,
            f64::NEG_INFINITY,
            9.223372036854776e18,
            -9.223372036854776e18,
        ]);
        for _ in 0..n {
            ints.push(rng.next() as i64 >> rng.below(64));
            let r = f64::from_bits(rng.next());
            if !r.is_nan() {
                reals.push(r);
            }
            reals.push((rng.next() as i64 >> rng.below(64)) as f64 / 1000.0);
        }
        (texts, ints, reals)
    }

    #[test]
    fn owned_reads_match_sqlite_for_numbers() {
        let mut rng = Rng::new(20261014);
        let (_, ints, reals) = every_class(&mut rng, 20_000);
        let inputs: Vec<Input<'_>> = ints
            .iter()
            .map(|&i| Input::Int(i))
            .chain(reals.iter().map(|&r| Input::Real(r)))
            .collect();
        assert!(compare(&inputs, false).is_empty());
    }

    /// Hand-picked edge cases (from the earlier SQLite-delegating version).
    const EDGE_CASES: &[&str] = &[
        "",
        " ",
        "0",
        "-0",
        "+0",
        "42",
        " 42 ",
        "\x0b7",
        "\x0c7",
        "-17abc",
        "+5",
        "007",
        "1.5",
        "-1.5",
        ".5",
        "-.5",
        "1.",
        ".",
        "-",
        "+",
        "e5",
        "1e5",
        "1E-3",
        "1e",
        "1e+",
        "2.5e2x",
        "1.9e3",
        "abc",
        "0x10",
        "inf",
        "nan",
        "-inf",
        "9223372036854775807",
        "9223372036854775808",
        "-9223372036854775808",
        "-9223372036854775809",
        "18446744073709551615",
        "18446744073709551616",
        "99999999999999999999999999",
        "-99999999999999999999999999",
        "1e400",
        "-1e400",
        "0.30000000000000004441",
        "123456789012345678901234567890",
        "2.2250738585072011e-308",
        "4.9406564584124654e-324",
        "1.7976931348623157e308",
        "1e-400",
        "3.14159",
        "0.1",
        "123456.789e-3",
        "0000000000000000000000000000001",
        "1e99999999999999999999",
        // multibyte UTF-8 inside and after the numeric prefix
        "12\u{e9}34",
        "\u{e9}12",
        "1.5\u{1f600}",
        "-3\u{4e2d}e2",
        "7e\u{e9}",
    ];

    /// INTEGER and TEXT reads must be exact; `double` of TEXT/BLOB may differ
    /// from `sqlite3AtoF` by at most `MAX_ULPS`. Prints every difference so the
    /// count can be reported (`cargo test -p sqlx-sqlite -- --nocapture`).
    #[test]
    fn text_and_blob_reads_match_sqlite_within_max_ulps() {
        let mut rng = Rng::new(20261015);
        let (texts, _, _) = every_class(&mut rng, 100_000);
        let long = "7".repeat(200_000);
        let long_cases = [long.clone(), format!("-{long}"), format!("0.{long}e-5")];
        let non_utf8: &[u8] = &[b'1', b'2', 0xff, b'3'];
        let all: Vec<&[u8]> = EDGE_CASES
            .iter()
            .map(|t| t.as_bytes())
            .chain(texts.iter().map(String::as_bytes))
            .chain(long_cases.iter().map(String::as_bytes))
            .chain([non_utf8])
            .collect();
        let inputs: Vec<Input<'_>> = all
            .iter()
            .flat_map(|&t| [Input::Text(t), Input::Blob(t)])
            .collect();
        let started = std::time::Instant::now();
        let diffs = compare(&inputs, true);
        assert!(started.elapsed() < std::time::Duration::from_secs(20));
        let mut by_ulps = std::collections::BTreeMap::new();
        for (_, ours, sqlite) in &diffs {
            *by_ulps
                .entry(ours.to_bits().abs_diff(sqlite.to_bits()))
                .or_insert(0) += 1;
        }
        println!(
            "double differs from SQLite on {} of {} TEXT/BLOB inputs; count by ulp distance: {by_ulps:?}",
            diffs.len(),
            inputs.len()
        );
        for (shown, ours, sqlite) in &diffs {
            println!(
                "  {shown}: ours {ours:e} ({:#018x}) sqlite {sqlite:e} ({:#018x})",
                ours.to_bits(),
                sqlite.to_bits()
            );
        }
    }
}
