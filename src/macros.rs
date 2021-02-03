#[macro_export]
macro_rules! query_stream [
    (
        $pool:expr,
        $sql:literal,
        $( $arg:expr ),*
    ) => ({
        $crate::RowStream::make(
            $pool,
            |pool| {
                sqlx::query(sql)
                    $( .bind($arg) )*
                    .fetch(pool)
            }
        )
    });
    (
        $struct_name:ident,
        $pool:expr,
        $sql:expr,
        $( $arg:expr ),*
    ) => ({
        $crate::RowWStmtStream::make(
            $pool,
            $sql,
            |pool,sql| {
                sqlx::query(sql)
                    $( .bind($arg) )*
                    .fetch(pool)
            }
        )
    });
];

#[macro_export]
macro_rules! query_as_stream [
    (
        $struct_name:ident,
        $pool:expr,
        $sql:literal,
        $( $arg:expr ),*
    ) => ({
        $crate::RowWStmtStream::make(
            $pool,
            $sql.to_string(),
            |pool,_sql| {
                sqlx::query_as!(
                    $struct_name,
                    $sql,
                    $( $arg ),*
                )
                    .fetch(pool)
            }
        )
    });
    (
        $struct_name:ident,
        $pool:expr,
        $sql:expr,
        $( $arg:expr ),*
    ) => ({
        $crate::RowWStmtStream::make(
            $pool,
            $sql,
            |pool,sql| {
                sqlx::query_as::<sqlx::postgres::Postgres, $struct_name>(sql)
                    $( .bind($arg) )*
                    .fetch(pool)
            }
        )
    });
];

#[macro_export]
macro_rules! query_as_byte_stream [
    (
        $struct_name:ident,
        $pool:expr,
        $sql:literal,
        $fn:expr,
        $( $arg:expr ),*
    ) => ({
        $crate::ByteStream::make(
            $crate::RowStream::make(
                $pool,
                |pool| {
                    sqlx::query_as!(
                        $struct_name,
                        $sql,
                        $( $arg ),*
                    )
                        .fetch(pool)
                }
            ),
            $fn,
        )
    });
    (
        $struct_name:ident,
        $pool:expr,
        $sql:expr,
        $fn:expr,
        $( $arg:expr ),*
    ) => ({
        $crate::ByteStream::make(
            $crate::RowWStmtStream::make(
                $pool,
                $sql,
                |pool,sql| {
                    sqlx::query_as::<sqlx::postgres::Postgres, $struct_name>(sql)
                        $( .bind($arg) )*
                        .fetch(pool)
                },
            ),
            $fn,
        )
    });
];

#[macro_export]
macro_rules! query_byte_stream [
    (
        $pool:expr,
        $sql:literal,
        $fn:expr,
        $( $arg:expr ),*
    ) => ({
        $crate::ByteStream::make(
            $crate::RowStream::make(
                $pool,
                |pool| {
                    sqlx::query!(
                        $sql,
                        $( $arg ),*
                    )
                        .fetch(pool)
                }
            ),
            $fn,
        )
    });
    (
        $pool:expr,
        $sql:expr,
        $fn:expr,
        $( $arg:expr ),*
    ) => ({
        $crate::ByteStream::make(
            $crate::RowWStmtStream::make(
                $pool,
                $sql,
                |pool,sql| {
                    sqlx::query::<sqlx::postgres::Postgres>(sql)
                        $( .bind($arg) )*
                        .fetch(pool)
                },
            ),
            $fn,
        )
    });
];
