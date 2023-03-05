use crate::plan::{OutputCsv, OutputJson, OutputParquet};

#[derive(Clone, Debug)]
pub struct CsvOutputConfig {
    pub path: String,
    pub overwrite: bool,
}
impl CsvOutputConfig {
    pub fn new(conf: &OutputCsv) -> CsvOutputConfig
    {
        CsvOutputConfig {
            path: conf.path.clone(),
            overwrite: conf.overwrite.unwrap_or(false),
        }
    }
}

#[derive(Clone, Debug)]
pub struct JsonOutputConfig {
    pub path: String,
    pub overwrite: bool,
}
impl JsonOutputConfig {
    pub fn new(conf: &OutputJson) -> JsonOutputConfig
    {
        JsonOutputConfig {
            path: conf.path.clone(),
            overwrite: conf.overwrite.unwrap_or(false),
        }
    }
}

#[derive(Clone, Debug)]
pub struct ParquetOutputConfig {
    pub path: String,
    pub overwrite: bool,
}
impl ParquetOutputConfig {
    pub fn new(conf: &OutputParquet) -> ParquetOutputConfig
    {
        ParquetOutputConfig {
            path: conf.path.clone(),
            overwrite: conf.overwrite.unwrap_or(false),
        }
    }
}