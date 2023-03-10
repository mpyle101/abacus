use std::ffi::OsStr;
use std::fs;
use std::path::Path;
use std::sync::Arc;

use datafusion::arrow::csv::Writer as CsvWriter;
use datafusion::arrow::datatypes::Schema;
use datafusion::arrow::json::LineDelimitedWriter as JsonWriter;
use datafusion::error::Result;
use datafusion::execution::context::SessionContext;
use datafusion::execution::options::{AvroReadOptions, CsvReadOptions, ParquetReadOptions};
use datafusion::prelude::{col, DataFrame};
use datafusion::parquet::arrow::ArrowWriter;
use datafusion::parquet::file::properties::WriterProperties;

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
    config: &CsvExportConfig
) -> Result<Option<DataFrame>>
{
    let df = data.left.take().unwrap();

    let path = Path::new(&config.path);
    if let Some("csv") = path.extension().and_then(OsStr::to_str) {
        if config.overwrite {
            let _ = fs::remove_file(path);
        }
        let file = fs::File::create(path)?;
        let mut writer = CsvWriter::new(file);
        let batches = df.collect().await?;
        batches.iter().for_each(|batch| { writer.write(batch).unwrap(); });
    } else {
        if config.overwrite {
            let _ = fs::remove_dir_all(path);
        }
        df.write_csv(&config.path).await?;
    }

    Ok(None)
}

pub async fn write_json(
    data: &mut Data,
    config: &JsonExportConfig
) -> Result<Option<DataFrame>>
{
    let df = data.left.take().unwrap();

    let path = Path::new(&config.path);
    if let Some("json") = path.extension().and_then(OsStr::to_str) {
        if config.overwrite {
            let _ = fs::remove_file(path);
        }
        let file = fs::File::create(path)?;
        let mut writer = JsonWriter::new(file);
        let batches = df.collect().await?;
        writer.write_batches(&batches)?;
        writer.finish()?;
    } else {
        if config.overwrite {
            let _ = fs::remove_dir_all(path);
        }
        df.write_json(&config.path).await?;
    }

    Ok(None)
}

pub async fn write_parquet(
    data: &mut Data,
    config: &ParquetExportConfig
) -> Result<Option<DataFrame>>
{
    let df = data.left.take().unwrap();
    let props = Some(WriterProperties::builder()
            .set_compression(config.compress)
            .build());

    let path = Path::new(&config.path);
    if let Some("parquet") = path.extension().and_then(OsStr::to_str) {
        if config.overwrite {
            let _ = fs::remove_file(path);
        }
        let file   = fs::File::create(path)?;
        let schema = df.schema().try_into().unwrap(); // can never fail
        let mut writer = ArrowWriter::try_new(file, Arc::new(schema), props)?;
        let batches = df.collect().await?;
        batches.iter().for_each(|batch| { writer.write(batch).unwrap(); });
        writer.close()?;
    } else {
        if config.overwrite {
            let _ = fs::remove_dir_all(path);
        }
        df.write_parquet(&config.path, props).await?;
    }

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

pub fn project(data: &mut Data, config: &MapConfig) -> Result<Option<DataFrame>>
{
    let df = data.left.take().unwrap();

    Ok(Some(df.select(config.exprs.clone())?))
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

pub fn sort(data: &mut Data, config: &SortConfig) -> Result<Option<DataFrame>>
{
    let df = data.left.take().unwrap();

    Ok(Some(df.sort(config.exprs.clone())?))
}

pub fn summarize(data: &mut Data, config: &SummarizeConfig) -> Result<Option<DataFrame>>
{
    let df = data.left.take().unwrap();

    Ok(Some(df.aggregate(config.group.clone(), config.aggr.clone())?))
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
