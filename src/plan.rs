use std::collections::HashMap;
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
pub struct InputCsv {
    pub id: String,
    pub path: String,
}

#[derive(Debug, Deserialize)]
pub struct InputAvro {
    pub id: String,
    pub path: String,
}

#[derive(Debug, Deserialize)]
pub struct InputParquet {
    pub id: String,
    pub path: String,
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

    #[serde(default)]
    pub overwrite: bool,
}

#[derive(Debug, Deserialize)]
pub struct OutputJson {
    pub id: String,
    pub path: String,

    #[serde(default)]
    pub overwrite: bool,
}

#[derive(Debug, Deserialize)]
pub struct OutputParquet {
    pub id: String,
    pub path: String,

    #[serde(default)]
    pub overwrite: bool,
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
    pub id: String
}