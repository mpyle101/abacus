use std::borrow::Cow;
use std::collections::HashMap;
use datafusion::arrow::datatypes::{DataType, TimeUnit};
use datafusion::parquet::basic::Compression;
use serde::Deserialize;

use crate::expr::Expression;

#[derive(Clone, Copy, Debug, Deserialize)]
#[allow(non_camel_case_types)]
pub enum ParquetCompression {
    brotli,
    gzip,
    lzo,
    lz4,
    lz4_raw,
    snappy,
    zstd,
}
#[allow(clippy::from_over_into)]
impl Into<Compression> for ParquetCompression {
    fn into(self) -> Compression
    {
        use datafusion::parquet::basic::{BrotliLevel, GzipLevel, ZstdLevel};

        match self {
            ParquetCompression::brotli  => 
                Compression::BROTLI(BrotliLevel::try_new(4).unwrap()),
            ParquetCompression::gzip    => Compression::GZIP(GzipLevel::default()),
            ParquetCompression::lzo     => Compression::LZO,
            ParquetCompression::lz4     => Compression::LZ4,
            ParquetCompression::lz4_raw => Compression::LZ4_RAW,
            ParquetCompression::zstd    =>
                Compression::ZSTD(ZstdLevel::try_new(11).unwrap()),
            ParquetCompression::snappy  => Compression::SNAPPY,
        }
    }
}

#[derive(Clone, Copy, Debug, Default, Deserialize)]
#[allow(non_camel_case_types)]
pub enum InputSide {
    #[default]
    left,
    right,
}

#[derive(Debug, Deserialize)]
pub struct Plan<'a> {
    pub id: &'a str,
    pub name: &'a str,
    pub links: Vec<Link<'a>>,
    pub tools: Vec<Tool<'a>>,
}

#[derive(Debug, Deserialize)]
pub struct Link<'a> {
    pub src: &'a str,
    pub dst: &'a str,

    #[serde(default)]
    pub input: InputSide,
}

#[derive(Debug, Deserialize)]
#[serde(tag="tool")]
#[allow(non_camel_case_types)]
pub enum Tool<'a> {
    #[serde(borrow)]
    import(Import<'a>),
    export(Export<'a>),
    distinct(Generic<'a>),
    difference(Generic<'a>),
    filter(Filter<'a>),
    intersect(Generic<'a>),
    join(Join<'a>),
    map(Map<'a>),
    select(Select<'a>),
    sort(Sort<'a>),
    summarize(Summarize<'a>),
    union(Union<'a>),
}
impl<'a> Tool<'a> {
    pub fn id(&self) -> &'a str
    {
        use Tool::*;

        match self {
            import(tool)     => tool.id(),
            export(tool)     => tool.id(),
            distinct(tool)   => tool.id,
            difference(tool) => tool.id,
            filter(tool)     => tool.id,
            intersect(tool)  => tool.id,
            join(tool)       => tool.id,
            map(tool)        => tool.id,
            select(tool)     => tool.id,
            sort(tool)       => tool.id,
            summarize(tool)  => tool.id,
            union(tool)      => tool.id,
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(tag="format")]
#[allow(non_camel_case_types)]
pub enum Import<'a> {
    #[serde(borrow)]
    csv(ImportCsv<'a>),
    avro(ImportAvro<'a>),
    parquet(ImportParquet<'a>),
}
impl<'a> Import<'a> {
    pub fn id(&self) -> &'a str
    {
        match self {
            Import::csv(tool)     => tool.id,
            Import::avro(tool)    => tool.id,
            Import::parquet(tool) => tool.id,
        }
    }
}

#[derive(Clone, Copy, Debug, Deserialize)]
#[allow(non_camel_case_types)]
pub enum SchemaDataType {
    utf8, bool, null, ts, ms, ns,
    i8, i16, i32, i64,
    u8, u16, u32, u64,
    f16, f32, f64, date,
}
#[allow(clippy::from_over_into)]
impl Into<DataType> for SchemaDataType {
    fn into(self) -> DataType
    {
        match self {
            SchemaDataType::ts   => DataType::Timestamp(TimeUnit::Second, None),
            SchemaDataType::ms   => DataType::Timestamp(TimeUnit::Millisecond, None),
            SchemaDataType::ns   => DataType::Timestamp(TimeUnit::Nanosecond, None),
            SchemaDataType::utf8 => DataType::Utf8,
            SchemaDataType::bool => DataType::Boolean,
            SchemaDataType::null => DataType::Null,
            SchemaDataType::i8   => DataType::Int8,
            SchemaDataType::i16  => DataType::Int16,
            SchemaDataType::i32  => DataType::Int32,
            SchemaDataType::i64  => DataType::Int64,
            SchemaDataType::u8   => DataType::UInt8,
            SchemaDataType::u16  => DataType::UInt16,
            SchemaDataType::u32  => DataType::UInt32,
            SchemaDataType::u64  => DataType::UInt64,
            SchemaDataType::f16  => DataType::Float16,
            SchemaDataType::f32  => DataType::Float32,
            SchemaDataType::f64  => DataType::Float64,
            SchemaDataType::date => DataType::Date32,
        }
    }
}

#[derive(Debug, Deserialize)]
pub struct SchemaField<'a> {
    pub column: &'a str,
    pub nullable: Option<bool>,

    #[serde(rename(deserialize = "type"))]
    pub variant: SchemaDataType,
}

#[derive(Clone, Debug, Deserialize)]
pub struct Sql<'a> {
    pub stmt: Cow<'a, str>,
    pub table: &'a str,
}

#[derive(Debug, Deserialize)]
pub struct ImportCsv<'a> {
    pub id: &'a str,
    pub path: &'a str,
    pub limit: Option<usize>,
    pub header: Option<bool>,
    pub delimiter: Option<u8>,
    pub schema: Option<Vec<SchemaField<'a>>>,
    pub sql: Option<Sql<'a>>,
}

#[derive(Debug, Deserialize)]
pub struct ImportAvro<'a> {
    pub id: &'a str,
    pub path: &'a str,
    pub limit: Option<usize>,
    pub sql: Option<Sql<'a>>,
}

#[derive(Debug, Deserialize)]
pub struct ImportParquet<'a> {
    pub id: &'a str,
    pub path: &'a str,
    pub limit: Option<usize>,
    pub sql: Option<Sql<'a>>,
}

#[derive(Debug, Deserialize)]
#[serde(tag="format")]
#[allow(non_camel_case_types)]
pub enum Export<'a> {
    #[serde(borrow)]
    csv(ExportCsv<'a>),
    json(ExportJson<'a>),
    parquet(ExportParquet<'a>),
}
impl<'a> Export<'a> {
    pub fn id(&self) -> &'a str
    {
        match self {
            Export::csv(tool)     => tool.id,
            Export::json(tool)    => tool.id,
            Export::parquet(tool) => tool.id,
        }
    }
}

#[derive(Debug, Deserialize)]
pub struct ExportCsv<'a> {
    pub id: &'a str,
    pub path: &'a str,
    pub overwrite: Option<bool>,
}

#[derive(Debug, Deserialize)]
pub struct ExportJson<'a> {
    pub id: &'a str,
    pub path: &'a str,
    pub overwrite: Option<bool>,
}

#[derive(Debug, Deserialize)]
pub struct ExportParquet<'a> {
    pub id: &'a str,
    pub path: &'a str,
    pub compress: Option<ParquetCompression>,
    pub overwrite: Option<bool>,
}

#[derive(Debug, Deserialize)]
pub struct Generic<'a> {
    pub id: &'a str,
}

#[derive(Debug, Deserialize)]
pub struct Filter<'a> {
    pub id: &'a str,
    pub expr: Expression<'a>,
}

#[derive(Debug, Deserialize)]
pub struct Join<'a> {
    pub id: &'a str,
    pub lt: Vec<&'a str>,
    pub rt: Vec<&'a str>,

    #[serde(rename(deserialize = "type"))]
    pub variant: JoinType,
}

#[derive(Debug, Deserialize)]
pub struct Map<'a> {
    pub id: &'a str,
    pub exprs: Vec<Expression<'a>>,
}

#[derive(Debug, Deserialize)]
pub struct Select<'a> {
    pub id: &'a str,
    pub columns: Vec<&'a str>,
    pub aliases: HashMap<&'a str, &'a str>,
}

#[derive(Debug, Deserialize)]
pub struct Sort<'a> {
    pub id: &'a str,
    pub exprs: Vec<SortExpression<'a>>,
}

#[derive(Debug, Deserialize)]
pub struct Summarize<'a> {
    pub id: &'a str,
    pub aggr: Vec<Expression<'a>>,
    pub group: Vec<Expression<'a>>,
}

#[derive(Debug, Deserialize)]
pub struct Union<'a> {
    pub id: &'a str,
    pub distinct: Option<bool>,
}

#[derive(Debug, Deserialize)]
pub struct SortExpression<'a> {
    pub asc: Option<bool>,
    pub nulls_first: Option<bool>,

    #[serde(borrow)]
    pub expr: Expression<'a>,
}

#[derive(Debug, Deserialize)]
#[allow(non_camel_case_types)]
pub enum JoinType {
    inner,
    left,
    right,
    full,
    left_semi,
    right_semi,
    left_anti,
    right_anti,
}
