use std::borrow::Cow;
use std::collections::HashMap;
use datafusion::arrow::datatypes::{DataType, TimeUnit};
use datafusion::logical_expr::Operator;
use datafusion::parquet::basic::Compression;
use datafusion::prelude::*;
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
    f64(f64),
    i32(i32),
    i64(i64),
    str(&'a str),
    abs(Box<Expression<'a>>),
    acos(Box<Expression<'a>>),
    asin(Box<Expression<'a>>),
    atan(Box<Expression<'a>>),
    not(Box<Expression<'a>>),
    eq(Box<[Expression<'a>;2]>),
    ne(Box<[Expression<'a>;2]>),
    gt(Box<[Expression<'a>;2]>),
    gte(Box<[Expression<'a>;2]>),
    lt(Box<[Expression<'a>;2]>),
    lte(Box<[Expression<'a>;2]>),
    add(Box<[Expression<'a>;2]>),
    sub(Box<[Expression<'a>;2]>),
    mul(Box<[Expression<'a>;2]>),
    div(Box<[Expression<'a>;2]>),
    avg(Vec<Expression<'a>>),
    and(Vec<Expression<'a>>),
    or(Vec<Expression<'a>>),
    min(Vec<Expression<'a>>),
    max(Vec<Expression<'a>>),
    sum(Vec<Expression<'a>>),
    stddev(Vec<Expression<'a>>),

    #[serde(rename(deserialize = "true"))]
    is_true(Box<Expression<'a>>),

    #[serde(rename(deserialize = "false"))]
    is_false(Box<Expression<'a>>),

    #[serde(rename(deserialize = "prod"))]
    product(Vec<Expression<'a>>),

    #[serde(rename(deserialize = "mod"))]
    modulus(Box<[Expression<'a>;2]>),

    cast(Box<Expression<'a>>, SchemaDataType),
    sort(Box<Expression<'a>>, bool, bool),
}

pub fn convert(expr: &Expression) -> Expr
{
    match expr {
        Expression::f32(v)  => lit(*v),
        Expression::f64(v)  => lit(*v),
        Expression::i32(v)  => lit(*v),
        Expression::i64(v)  => lit(*v),
        Expression::str(v) => lit(*v),
        Expression::col(v) => col(format!(r#""{v}""#)),
        Expression::abs(expr)  => abs(convert(expr)),
        Expression::acos(expr) => acos(convert(expr)),
        Expression::asin(expr) => atan(convert(expr)),
        Expression::atan(expr) => atan(convert(expr)),
        Expression::not(expr)  => not(convert(expr)),
        Expression::and(exprs) => exprs.iter().map(convert).reduce(and).unwrap(),
        Expression::or(exprs)  => exprs.iter().map(convert).reduce(or).unwrap(),
        Expression::avg(exprs) => avg(make_array(exprs.iter().map(convert).collect())),
        Expression::min(exprs) => min(make_array(exprs.iter().map(convert).collect())),
        Expression::max(exprs) => max(make_array(exprs.iter().map(convert).collect())),
        Expression::sum(exprs) => sum(make_array(exprs.iter().map(convert).collect())),
        Expression::eq(exprs)  => binary_expr(convert(&exprs[0]), Operator::Eq, convert(&exprs[1])),
        Expression::ne(exprs)  => binary_expr(convert(&exprs[0]), Operator::NotEq, convert(&exprs[1])),
        Expression::gt(exprs)  => binary_expr(convert(&exprs[0]), Operator::Gt, convert(&exprs[1])),
        Expression::gte(exprs) => binary_expr(convert(&exprs[0]), Operator::GtEq, convert(&exprs[1])),
        Expression::lt(exprs)  => binary_expr(convert(&exprs[0]), Operator::Lt, convert(&exprs[1])),
        Expression::lte(exprs) => binary_expr(convert(&exprs[0]), Operator::LtEq, convert(&exprs[1])),
        Expression::add(exprs) => binary_expr(convert(&exprs[0]), Operator::Plus, convert(&exprs[1])),
        Expression::sub(exprs) => binary_expr(convert(&exprs[0]), Operator::Minus, convert(&exprs[1])),
        Expression::mul(exprs) => binary_expr(convert(&exprs[0]), Operator::Multiply, convert(&exprs[1])),
        Expression::div(exprs) => binary_expr(convert(&exprs[0]), Operator::Divide, convert(&exprs[1])),
        
        Expression::is_true(expr)  => is_true(convert(expr)),
        Expression::is_false(expr) => is_false(convert(expr)),
        Expression::stddev(exprs)  => stddev(make_array(exprs.iter().map(convert).collect())),
        Expression::modulus(exprs) => binary_expr(convert(&exprs[0]), Operator::Modulo, convert(&exprs[1])),
        Expression::product(exprs) => exprs.iter().map(convert).reduce(|a, b| a * b).unwrap(),
        Expression::cast(expr, dtype) => cast(convert(expr), (*dtype).into()),
        Expression::sort(expr, asc, nulls_first) => convert(expr).sort(*asc, *nulls_first),
    }
}
