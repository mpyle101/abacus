use std::fs;

use datafusion::arrow::datatypes::Schema;
use datafusion::error::Result;
use datafusion::execution::context::SessionContext;
use datafusion::execution::options::{AvroReadOptions, CsvReadOptions, ParquetReadOptions};
use datafusion::prelude::{col, DataFrame};

use crate::config::*;
use crate::plans::InputSide;

#[derive(Clone, Debug, Default)]
pub struct Data {
    pub left: Option<DataFrame>,
    pub right: Option<DataFrame>,
}
impl Data {
    pub fn set(&mut self, side: InputSide, df: DataFrame)
    {
        match side {
            InputSide::left  => self.left  = Some(df),
            InputSide::right => self.right = Some(df),
        }
    }
}

pub async fn read_csv(ctx: SessionContext, config: &CsvImportConfig) -> Result<Option<DataFrame>>
{
    let schema = config.fields.as_ref().map(
        |fields| Schema::new(fields.clone())
    );
    let options = CsvReadOptions {
        schema: schema.as_ref(),
        delimiter: config.delimiter,
        has_header: config.header,
        ..Default::default()
    };

    let df = if let Some(sql) = &config.sql {
        ctx.register_csv(&sql.table, &config.path, options).await?;
        ctx.sql(&sql.stmt).await?
    } else {
        ctx.read_csv(&config.path, options).await?
    };
    
    Ok(Some(df.limit(0, config.limit)?))
}

pub async fn read_avro(ctx: SessionContext, config: &AvroImportConfig) -> Result<Option<DataFrame>>
{
    let options = AvroReadOptions::default();
    let df = if let Some(sql) = &config.sql {
        ctx.register_avro(&sql.table, &config.path, options).await?;
        ctx.sql(&sql.stmt).await?
    } else {
        ctx.read_avro(&config.path, options).await?
    };

    Ok(Some(df.limit(0, config.limit)?))
}

pub async fn read_parquet(ctx: SessionContext, config: &ParquetImportConfig) -> Result<Option<DataFrame>>
{
    let options = ParquetReadOptions::default();
    let df = if let Some(sql) = &config.sql {
        ctx.register_parquet(&sql.table, &config.path, options).await?;
        ctx.sql(&sql.stmt).await?
    } else {
        ctx.read_parquet(&config.path, options).await?
    };
    Ok(Some(df.limit(0, config.limit)?))
}

pub async fn write_csv(
    data: &mut Data,
    ctx: SessionContext,
    config: &CsvExportConfig
) -> Result<Option<DataFrame>>
{
    if config.overwrite {
        let _ = fs::remove_dir_all(&config.path);
    }
    let plan = data.left.take().unwrap().create_physical_plan().await?;
    ctx.write_csv(plan, &config.path).await?;

    Ok(None)
}

pub async fn write_json(
    data: &mut Data,
    ctx: SessionContext,
    config: &JsonExportConfig
) -> Result<Option<DataFrame>>
{
    if config.overwrite {
        let _ = fs::remove_dir_all(&config.path);
    }
    let plan = data.left.take().unwrap().create_physical_plan().await?;
    ctx.write_json(plan, &config.path).await?;

    Ok(None)
}

pub async fn write_parquet(
    data: &mut Data,
    ctx: SessionContext,
    config: &ParquetExportConfig
) -> Result<Option<DataFrame>>
{
    if config.overwrite {
        let _ = fs::remove_dir_all(&config.path);
    }
    let plan = data.left.take().unwrap().create_physical_plan().await?;
    ctx.write_parquet(plan, &config.path, None).await?;

    Ok(None)
}

pub fn difference(data: &mut Data) -> Result<Option<DataFrame>>
{
    let left  = data.left.take().unwrap();
    let right = data.right.take().unwrap();

    Ok(Some(left.except(right)?))
}

pub fn distinct(data: &mut Data) -> Result<Option<DataFrame>>
{
    let df = data.left.take().unwrap();

    Ok(Some(df.distinct()?))
}

pub fn intersect(data: &mut Data) -> Result<Option<DataFrame>>
{
    let left  = data.left.take().unwrap();
    let right = data.right.take().unwrap();

    Ok(Some(left.intersect(right)?))
}

pub fn filter(data: &mut Data, config: &FilterConfig) -> Result<Option<DataFrame>>
{
    let df = data.left.take().unwrap();

    Ok(Some(df.filter(config.expr.clone())?))
}

pub fn join(data: &mut Data, config: &JoinConfig) -> Result<Option<DataFrame>>
{
    let left  = data.left.take().unwrap();
    let right = data.right.take().unwrap();
    let lt_cols = config.left_cols.iter()
        .map(|c| c.as_ref())
        .collect::<Vec<_>>();
    let rt_cols = config.right_cols.iter()
        .map(|c| c.as_ref())
        .collect::<Vec<_>>();
    let frame = left.join(right, config.join_type, &lt_cols, &rt_cols, None)?;

    Ok(Some(frame))
}

pub fn select(data: &mut Data, config: &SelectConfig) -> Result<Option<DataFrame>>
{
    let df = data.left.take().unwrap();
    let exprs = config.columns.iter()
        .map(|c| if let Some(alias) = config.aliases.get(c) {
                col(c).alias(alias)
            } else {
                col(c)
            }
        )
        .collect::<Vec<_>>();

    Ok(Some(df.select(exprs)?))
}

pub fn union(data: &mut Data, config: &UnionConfig) -> Result<Option<DataFrame>>
{
    let df = data.left.take().unwrap();
    let other = data.right.take().unwrap();
    let frame = if config.distinct {
        df.union_distinct(other)?
    } else {
        df.union(other)?
    };

    Ok(Some(frame))
}
