use std::collections::HashMap;
use datafusion::arrow::datatypes::{DataType, TimeUnit};
use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct Plan {
    pub id: String,
    pub name: String,
    pub links: Vec<Link>,
    pub tools: Vec<Tool>,
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
pub struct Link {
    pub src: String,
    pub dst: String,

    #[serde(default)]
    pub input: InputSide,
}

#[derive(Debug, Deserialize)]
#[serde(tag="tool")]
#[allow(non_camel_case_types)]
pub enum Tool {
    input(Input),
    output(Output),
    join(Join),
    select(Select),
    union(Union)
}

impl Tool {
    pub fn id(&self) -> String
    {
        use Tool::*;

        match self {
            input(tool)  => tool.id(),
            output(tool) => tool.id(),
            join(tool)   => tool.id.clone(),
            select(tool) => tool.id.clone(),
            union(tool)  => tool.id.clone(),
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(tag="format")]
#[allow(non_camel_case_types)]
pub enum Input {
    csv(InputCsv),
    avro(InputAvro),
    parquet(InputParquet),
}

impl Input {
    pub fn id(&self) -> String
    {
        use Input::*;

        match self {
            csv(tool)     => tool.id.clone(),
            avro(tool)    => tool.id.clone(),
            parquet(tool) => tool.id.clone(),
        }
    }
}

#[derive(Debug, Deserialize)]
#[allow(non_camel_case_types)]
pub enum SchemaDataType {
    utf8, bool, null, ts, ms, ns,
    i8, i16, i32, i64,
    u8, u16, u32, u64,
    f16, f32, f64,
}
#[allow(clippy::from_over_into)]
impl Into<DataType> for &SchemaDataType {
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
pub struct InputCsv {
    pub id: String,
    pub path: String,
    pub limit: Option<usize>,
    pub header: Option<bool>,
    pub delimiter: Option<u8>,
    pub schema: Option<Vec<HashMap<String, SchemaDataType>>>,
}

#[derive(Debug, Deserialize)]
pub struct InputAvro {
    pub id: String,
    pub path: String,
    pub limit: Option<usize>,
}

#[derive(Debug, Deserialize)]
pub struct InputParquet {
    pub id: String,
    pub path: String,
    pub limit: Option<usize>,
}

#[derive(Debug, Deserialize)]
#[serde(tag="format")]
#[allow(non_camel_case_types)]
pub enum Output {
    csv(OutputCsv),
    json(OutputJson),
    parquet(OutputParquet),
}

impl Output {
    pub fn id(&self) -> String
    {
        use Output::*;

        match self {
            csv(tool)     => tool.id.clone(),
            json(tool)    => tool.id.clone(),
            parquet(tool) => tool.id.clone(),
        }
    }
}

#[derive(Debug, Deserialize)]
pub struct OutputCsv {
    pub id: String,
    pub path: String,
    pub overwrite: Option<bool>,
}

#[derive(Debug, Deserialize)]
pub struct OutputJson {
    pub id: String,
    pub path: String,
    pub overwrite: Option<bool>,
}

#[derive(Debug, Deserialize)]
pub struct OutputParquet {
    pub id: String,
    pub path: String,
    pub overwrite: Option<bool>,
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
pub struct Join {
    pub id: String,
    pub lt: Vec<String>,
    pub rt: Vec<String>,

    #[serde(rename(deserialize = "type"))]
    pub variant: JoinType,
}

#[derive(Clone, Debug, Deserialize)]
pub struct Select {
    pub id: String,
    pub columns: Vec<String>,
    pub aliases: HashMap<String, String>,
}

#[derive(Clone, Debug, Deserialize)]
pub struct Union {
    pub id: String,
    pub distinct: Option<bool>,
}