use std::convert::From;
use crate::plans::{ImportAvro, ImportCsv, ImportParquet, Sql};
use datafusion::arrow::datatypes::Field;

#[derive(Clone, Debug)]
pub struct SqlConfig {
    pub stmt: String,
    pub table: String,
}
impl From<&Sql<'_>> for SqlConfig {
    fn from(sql: &Sql) -> SqlConfig
    {
        SqlConfig { stmt: sql.stmt.to_string(), table: sql.table.to_string() }
    }
}

#[derive(Clone, Debug)]
pub struct CsvImportConfig {
    pub path: String,
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
            path: config.path.to_string(),
            sql: config.sql.as_ref().map(|conf| conf.into()),
            limit: config.limit,
            header: config.header.unwrap_or(false), 
            delimiter: config.delimiter.unwrap_or(b','),
        }
    }
}

#[derive(Clone, Debug)]
pub struct AvroImportConfig {
    pub path: String,
    pub sql: Option<SqlConfig>,
    pub limit: Option<usize>,
}
impl From<&ImportAvro<'_>> for AvroImportConfig {
    fn from(config: &ImportAvro) -> AvroImportConfig
    {
        AvroImportConfig {
            path: config.path.to_string(),
            sql: config.sql.as_ref().map(|conf| conf.into()),
            limit: config.limit
        }
    }
}

#[derive(Clone, Debug)]
pub struct ParquetImportConfig {
    pub path: String,
    pub sql: Option<SqlConfig>,
    pub limit: Option<usize>,
}
impl From<&ImportParquet<'_>> for ParquetImportConfig {
    fn from(config: &ImportParquet) -> ParquetImportConfig
    {
        ParquetImportConfig {
            path: config.path.to_string(),
            sql: config.sql.as_ref().map(|conf| conf.into()),
            limit: config.limit
        }
    }
}
