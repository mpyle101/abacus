use std::ffi::OsStr;
use std::fs;
use std::path::Path;
use std::sync::Arc;

use datafusion::arrow::csv::Writer as CsvWriter;
use datafusion::arrow::datatypes::Schema;
use datafusion::arrow::json::LineDelimitedWriter as JsonWriter;
use datafusion::config::{ParquetOptions, TableParquetOptions};
use datafusion::dataframe::DataFrameWriteOptions;
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
        let mut recs = 0;
        let batches = df.collect().await?;
        batches.iter()
            .for_each(|batch| { recs += batch.num_rows(); writer.write(batch).unwrap(); });
        println!("{recs} records written to {:?}", path);
    } else {
        if config.overwrite {
            let _ = fs::remove_file(path);
        }
        let opts = DataFrameWriteOptions::new();
        df.write_csv(&config.path, opts, None).await?;
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
        let mut recs = 0;
        let batches = df.collect().await?;
        batches.iter()
            .for_each(|batch| { recs += batch.num_rows(); writer.write(batch).unwrap(); });
        writer.finish()?;
        println!("{recs} records written to {:?}", path);
    } else {
        let opts = DataFrameWriteOptions::new()
            .with_overwrite(config.overwrite);
        df.write_json(&config.path, opts, None).await?;
    }

    Ok(None)
}

pub async fn write_parquet(
    data: &mut Data,
    config: &ParquetExportConfig
) -> Result<Option<DataFrame>>
{
    let df = data.left.take().unwrap();
    let props = Some(
        TableParquetOptions {
            global: ParquetOptions {
                compression: Some(format!("{}", config.compress)),
                ..Default::default()
            },
            ..Default::default()
        });
    
    let path = Path::new(&config.path);
    if let Some("parquet") = path.extension().and_then(OsStr::to_str) {
        if config.overwrite {
            let _ = fs::remove_file(path);
        }
        let props = Some(WriterProperties::builder()
            .set_compression(config.compress)
            .build());
        let file   = fs::File::create(path)?;
        let schema = df.schema().into();
        let mut writer = ArrowWriter::try_new(file, Arc::new(schema), props)?;
        let mut recs = 0;
        let batches = df.collect().await?;
        batches.iter()
            .for_each(|batch| { recs += batch.num_rows(); writer.write(batch).unwrap(); });
        writer.close()?;
        println!("{recs} records written to {:?}", path);
    } else {
        let opts = DataFrameWriteOptions::new()
            .with_overwrite(config.overwrite);
        df.write_parquet(&config.path, opts, props).await?;
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
    let exprs = config.left_cols.iter().zip(config.right_cols.iter())
        .map(|(c1, c2)| col(c1).eq(col(format!(r#""{c2}""#))));

    Ok(Some(left.join_on(right, config.join_type, exprs)?))
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
    let left = data.left.take().unwrap();
    let right = data.right.take().unwrap();
    let df = if config.distinct {
        left.union_distinct(right)?
    } else {
        left.union(right)?
    };

    Ok(Some(df))
}
