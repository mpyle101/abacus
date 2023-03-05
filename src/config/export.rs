use crate::plan::{ExportCsv, ExportJson, ExportParquet};

#[derive(Clone, Debug)]
pub struct CsvExportConfig {
    pub path: String,
    pub overwrite: bool,
}
impl CsvExportConfig {
    pub fn new(conf: &ExportCsv) -> CsvExportConfig
    {
        CsvExportConfig {
            path: conf.path.clone(),
            overwrite: conf.overwrite.unwrap_or(false),
        }
    }
}

#[derive(Clone, Debug)]
pub struct JsonExportConfig {
    pub path: String,
    pub overwrite: bool,
}
impl JsonExportConfig {
    pub fn new(conf: &ExportJson) -> JsonExportConfig
    {
        JsonExportConfig {
            path: conf.path.clone(),
            overwrite: conf.overwrite.unwrap_or(false),
        }
    }
}

#[derive(Clone, Debug)]
pub struct ParquetExportConfig {
    pub path: String,
    pub overwrite: bool,
}
impl ParquetExportConfig {
    pub fn new(conf: &ExportParquet) -> ParquetExportConfig
    {
        ParquetExportConfig {
            path: conf.path.clone(),
            overwrite: conf.overwrite.unwrap_or(false),
        }
    }
}