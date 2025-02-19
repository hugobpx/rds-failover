use sqlx::migrate::MigrateError;
use sqlx::{PgPool, Row};

pub struct Pg {
    pool: PgPool,
}

impl Pg {
    pub async fn new(url: &str) -> sqlx::Result<Self> {
        let pool = PgPool::connect(url).await?;
        Ok(Self { pool })
    }

    pub async fn migrate(&self) -> Result<(), MigrateError> {
        sqlx::migrate!("./migrations").run(&self.pool).await
    }

    pub async fn write(&self, value: i64) -> sqlx::Result<()> {
        sqlx::query("INSERT INTO counter (value) VALUES ($1)")
            .bind(value)
            .execute(&self.pool)
            .await
            .map(|_| ())
    }

    pub async fn read_latest(&self) -> sqlx::Result<Option<(i64, i64)>> {
        let result = sqlx::query("SELECT * FROM counter ORDER BY value DESC LIMIT 1")
            .fetch_optional(&self.pool)
            .await?;
        match result {
            Some(row) => {
                let id: i64 = row.get("id");
                let value: i64 = row.get("value");
                Ok(Some((id, value)))
            }
            None => Ok(None),
        }
    }
}
