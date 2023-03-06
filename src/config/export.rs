use crate::plans::{ExportCsv, ExportJson, ExportParquet};

#[derive(Clone, Debug)]
pub struct CsvExportConfig {
    pub path: String,
    pub overwrite: bool,
}
impl CsvExportConfig {
    pub fn new(config: &ExportCsv) -> CsvExportConfig
    {
        CsvExportConfig {
            path: config.path.to_string(),
            overwrite: config.overwrite.unwrap_or(false),
        }
    }
}

#[derive(Clone, Debug)]
pub struct JsonExportConfig {
    pub path: String,
    pub overwrite: bool,
}
impl JsonExportConfig {
    pub fn new(config: &ExportJson) -> JsonExportConfig
    {
        JsonExportConfig {
            path: config.path.to_string(),
            overwrite: config.overwrite.unwrap_or(false),
        }
    }
}

#[derive(Clone, Debug)]
pub struct ParquetExportConfig {
    pub path: String,
    pub overwrite: bool,
}
impl ParquetExportConfig {
    pub fn new(config: &ExportParquet) -> ParquetExportConfig
    {
        ParquetExportConfig {
            path: config.path.to_string(),
            overwrite: config.overwrite.unwrap_or(false),
        }
    }
}