use std::borrow::Cow;
use std::collections::HashMap;
use std::convert::TryFrom;

use datafusion::arrow::datatypes::{DataType, TimeUnit};
use datafusion::parquet::basic::Compression;
use serde::Deserialize;

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
        match self {
            ParquetCompression::brotli  => Compression::BROTLI,
            ParquetCompression::gzip    => Compression::GZIP,
            ParquetCompression::lzo     => Compression::LZO,
            ParquetCompression::lz4     => Compression::LZ4,
            ParquetCompression::lz4_raw => Compression::LZ4_RAW,
            ParquetCompression::zstd    => Compression::ZSTD,
            ParquetCompression::snappy  => Compression::SNAPPY,
        }
    }
}

#[derive(Clone, Copy, Debug, Deserialize)]
#[allow(non_camel_case_types)]
pub enum InputSide {
    left,
    right,
}
impl Default for InputSide {
    fn default() -> Self { InputSide::left }
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
    f16, f32, f64,
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
    pub url: Url,
    pub delimiter: Option<u8>,
    pub header: Option<bool>,
    pub limit: Option<usize>,
    pub schema: Option<Vec<SchemaField<'a>>>,
    pub sql: Option<Sql<'a>>,
}
impl<'a> ImportCsv<'a> {
    pub fn url(&self) -> url::Url { self.url.0.clone() }
}

#[derive(Debug, Deserialize)]
pub struct ImportAvro<'a> {
    pub id: &'a str,
    pub url: Url,
    pub limit: Option<usize>,
    pub sql: Option<Sql<'a>>,
}
impl<'a> ImportAvro<'a> {
    pub fn url(&self) -> url::Url { self.url.0.clone() }
}

#[derive(Debug, Deserialize)]
pub struct ImportParquet<'a> {
    pub id: &'a str,
    pub url: Url,
    pub limit: Option<usize>,
    pub sql: Option<Sql<'a>>,
}
impl<'a> ImportParquet<'a> {
    pub fn url(&self) -> url::Url { self.url.0.clone() }
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
    pub url: Url,
    pub overwrite: Option<bool>,
}
impl<'a> ExportCsv<'a> {
    pub fn url(&self) -> url::Url { self.url.0.clone() }
}

#[derive(Debug, Deserialize)]
pub struct ExportJson<'a> {
    pub id: &'a str,
    pub url: Url,
    pub overwrite: Option<bool>,
}
impl<'a> ExportJson<'a> {
    pub fn url(&self) -> url::Url { self.url.0.clone() }
}

#[derive(Debug, Deserialize)]
pub struct ExportParquet<'a> {
    pub id: &'a str,
    pub url: Url,
    pub compress: Option<ParquetCompression>,
    pub overwrite: Option<bool>,
}
impl<'a> ExportParquet<'a> {
    pub fn url(&self) -> url::Url { self.url.0.clone() }
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
pub struct Select<'a> {
    pub id: &'a str,
    pub columns: Vec<&'a str>,
    pub aliases: HashMap<&'a str, &'a str>,
}

#[derive(Debug, Deserialize)]
pub struct Map<'a> {
    pub id: &'a str,
    pub exprs: Vec<Expression<'a>>,
}

#[derive(Debug, Deserialize)]
pub struct Sort<'a> {
    pub id: &'a str,
    pub exprs: Vec<Expression<'a>>,
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

#[derive(Debug, Deserialize)]
#[allow(non_camel_case_types)]
pub enum Expression<'a> {
    col(&'a str),
    f32(f32),
    i32(i32),
    abs(Box<Expression<'a>>),
    avg(Box<Expression<'a>>),
    acos(Box<Expression<'a>>),
    asin(Box<Expression<'a>>),
    atan(Box<Expression<'a>>),
    and(Vec<Expression<'a>>),
    or(Vec<Expression<'a>>),
    not(Box<Expression<'a>>),
    gt(Box<[Expression<'a>;2]>),
    gte(Box<[Expression<'a>;2]>),
    lt(Box<[Expression<'a>;2]>),
    lte(Box<[Expression<'a>;2]>),
    max(Vec<Expression<'a>>),
    min(Vec<Expression<'a>>),
    sum(Vec<Expression<'a>>),
    add(Box<[Expression<'a>;2]>),
    sub(Box<[Expression<'a>;2]>),
    mul(Box<[Expression<'a>;2]>),

    #[serde(rename(deserialize = "prd"))]
    product(Vec<Expression<'a>>),

    #[serde(rename(deserialize = "mod"))]
    modulus(Box<[Expression<'a>;2]>)
}

#[derive(Debug, Deserialize)]
struct UrlString<'a>(&'a str);

#[derive(Debug, Deserialize)]
#[serde(try_from = "UrlString")]
pub struct Url(url::Url);

impl TryFrom<UrlString<'_>> for Url {
    type Error = url::ParseError;

    fn try_from(other: UrlString) -> Result<Self, Self::Error>
    {
        Ok(Url(url::Url::parse(other.0)?))
    }
}
