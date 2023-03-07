use std::convert::From;

use datafusion::arrow::datatypes::Field;

use crate::plans::{ImportAvro, ImportCsv, ImportParquet, Sql};

#[derive(Clone, Debug)]
pub struct SqlConfig {
    pub stmt: String,
    pub table: String,
}
impl From<&Sql<'_>> for SqlConfig {
    fn from(sql: &Sql) -> SqlConfig
    {
        SqlConfig { stmt: sql.stmt.to_string(), table: sql.table.into() }
    }
}

#[derive(Clone, Debug)]
pub struct CsvImportConfig {
    pub url: url::Url,
    pub header: bool,
    pub delimiter: u8,
    pub sql: Option<SqlConfig>,
    pub limit: Option<usize>,
    pub fields: Option<Vec<Field>>,
}
impl From<&ImportCsv<'_>> for CsvImportConfig {
    fn from(config: &ImportCsv) -> CsvImportConfig
    {
        let fields = config.schema.as_ref().map(|v| v.iter()
            .map(|field| 
                Field::new(
                    field.column,
                    field.variant.into(),
                    field.nullable.unwrap_or(true)
                ))
            .collect());

        CsvImportConfig {
            fields,
            url: config.url(),
            sql: config.sql.as_ref().map(|conf| conf.into()),
            limit: config.limit,
            header: config.header.unwrap_or(false), 
            delimiter: config.delimiter.unwrap_or(b','),
        }
    }
}

#[derive(Clone, Debug)]
pub struct AvroImportConfig {
    pub url: url::Url,
    pub sql: Option<SqlConfig>,
    pub limit: Option<usize>,
}
impl From<&ImportAvro<'_>> for AvroImportConfig {
    fn from(config: &ImportAvro) -> Self
    {
        AvroImportConfig {
            url: config.url(),
            sql: config.sql.as_ref().map(|conf| conf.into()),
            limit: config.limit
        }
    }
}

#[derive(Clone, Debug)]
pub struct ParquetImportConfig {
    pub url: url::Url,
    pub sql: Option<SqlConfig>,
    pub limit: Option<usize>,
}
impl From<&ImportParquet<'_>> for ParquetImportConfig {
    fn from(config: &ImportParquet) -> Self
    {
        ParquetImportConfig {
            url: config.url(),
            sql: config.sql.as_ref().map(|conf| conf.into()),
            limit: config.limit
        }
    }
}
