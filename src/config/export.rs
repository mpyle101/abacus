use std::convert::From;
use datafusion::parquet::basic::Compression;

use crate::plans::{ExportCsv, ExportJson, ExportParquet};

#[derive(Clone, Debug)]
pub struct CsvExportConfig {
    pub url: url::Url,
    pub overwrite: bool,
}
impl From<&ExportCsv<'_>> for CsvExportConfig {
    fn from(config: &ExportCsv) -> CsvExportConfig
    {
        CsvExportConfig {
            url: config.url(),
            overwrite: config.overwrite.unwrap_or(false),
        }
    }
}

#[derive(Clone, Debug)]
pub struct JsonExportConfig {
    pub url: url::Url,
    pub overwrite: bool,
}
impl From<&ExportJson<'_>> for JsonExportConfig {
    fn from(config: &ExportJson) -> JsonExportConfig
    {
        JsonExportConfig {
            url: config.url(),
            overwrite: config.overwrite.unwrap_or(false),
        }
    }
}

#[derive(Clone, Debug)]
pub struct ParquetExportConfig {
    pub url: url::Url,
    pub compress: Compression,
    pub overwrite: bool,
}
impl From<&ExportParquet<'_>> for ParquetExportConfig {
    fn from(config: &ExportParquet) -> ParquetExportConfig
    {
        ParquetExportConfig {
            url: config.url(),
            compress: config.compress.map_or(Compression::UNCOMPRESSED, |v| v.into()),
            overwrite: config.overwrite.unwrap_or(false),
        }
    }
}