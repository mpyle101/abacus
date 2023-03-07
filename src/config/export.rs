use std::convert::From;
use crate::plans::{ExportCsv, ExportJson, ExportParquet};

#[derive(Clone, Debug)]
pub struct CsvExportConfig {
    pub path: String,
    pub overwrite: bool,
}
impl From<&ExportCsv<'_>> for CsvExportConfig {
    fn from(config: &ExportCsv) -> CsvExportConfig
    {
        CsvExportConfig {
            path: config.path.into(),
            overwrite: config.overwrite.unwrap_or(false),
        }
    }
}

#[derive(Clone, Debug)]
pub struct JsonExportConfig {
    pub path: String,
    pub overwrite: bool,
}
impl From<&ExportJson<'_>> for JsonExportConfig {
    fn from(config: &ExportJson) -> JsonExportConfig
    {
        JsonExportConfig {
            path: config.path.into(),
            overwrite: config.overwrite.unwrap_or(false),
        }
    }
}

#[derive(Clone, Debug)]
pub struct ParquetExportConfig {
    pub path: String,
    pub overwrite: bool,
}
impl From<&ExportParquet<'_>> for ParquetExportConfig {
    fn from(config: &ExportParquet) -> ParquetExportConfig
    {
        ParquetExportConfig {
            path: config.path.into(),
            overwrite: config.overwrite.unwrap_or(false),
        }
    }
}