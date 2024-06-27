use color_eyre::Result;
use hopsworks_core::controller::feature_store::query;
use sqlx::mysql::{MySqlPool, MySqlPoolOptions, MySqlRow};
use tokio::{sync::OnceCell, task::JoinHandle};
use log::debug;

static POOL: OnceCell<MySqlPool> = OnceCell::const_new();

pub async fn get_mysql_pool() -> &'static MySqlPool {
    match POOL.get() {
        Some(pool) => pool,
        None => panic!("First use async_sql::connect() to initialize the MySQL pool."),
    }
}

pub async fn connect(db_uri: &str) -> Result<()> {
    let pool = MySqlPoolOptions::new()
        .max_connections(5)
        .connect(db_uri)
        .await?;
    POOL.set(pool).map_err(|_| {
        color_eyre::Report::msg("Failed to set MySQL pool in OnceCell")
    })?;
    Ok(())
}

pub async fn disconnect() {
    POOL.get().map(|pool| {
        pool.close();
    });
}

pub async fn get_single_row_from_single_table(query: &str) -> Result<()> {
    let pool = get_mysql_pool().await;
    let row = sqlx::query(query)
        .fetch_one(pool)
        .await?;
    debug!("{:?}", row);
    Ok(())
}

pub async fn get_multiple_rows_from_single_table(query: &str) -> Result<()> {
    let pool = get_mysql_pool().await;
    let rows = sqlx::query(query)
        .fetch_all(pool)
        .await?;
    debug!("{:?}", rows);
    Ok(())
}

pub async fn fetch_one_with_many_queries(queries: &[String]) -> Result<()> {
    let pool = get_mysql_pool().await;
    let mut handles: Vec<JoinHandle<Result<Option<MySqlRow>>>> = Vec::with_capacity(queries.len());
    
    for query in queries {
        let query = query.to_string();
        let handle = tokio::spawn(async move {
            Ok(sqlx::query(&query).fetch_optional(pool).await?)
        });
        handles.push(handle);
    };
    let mut values = Vec::with_capacity(queries.len());
    for handle in handles {
        values.push(handle.await??);
    }
    
    debug!("{:?}", values);
    Ok(())
}

pub async fn fetch_all_with_many_queries(queries: &[String]) -> Result<()> {
    let pool = get_mysql_pool().await;
    let mut handles: Vec<JoinHandle<Result<Vec<MySqlRow>>>> = Vec::with_capacity(queries.len());
    
    for query in queries {
        let query = query.to_string();
        let handle = tokio::spawn(async move {
            Ok(sqlx::query(&query).fetch_all(pool).await?)
        });
        handles.push(handle);
    };
    let mut values = Vec::with_capacity(queries.len());
    for handle in handles {
        values.push(handle.await??);
    }
    
    debug!("{:?}", values);
    Ok(())
}




