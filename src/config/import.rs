use crate::plan::{ImportAvro, ImportCsv, ImportParquet, Sql};
use datafusion::arrow::datatypes::Field;

#[derive(Clone, Debug)]
#[allow(unused)]
pub struct CsvImportConfig {
    pub path: String,
    pub header: bool,
    pub delimiter: u8,
    pub sql: Option<Sql>,
    pub limit: Option<usize>,
    pub fields: Option<Vec<Field>>,
}
impl CsvImportConfig {
    pub fn new(config: &ImportCsv) -> CsvImportConfig
    {
        let fields = config.schema.as_ref().map(|v| v.iter()
            .map(|field| 
                Field::new(
                    field.column.clone(),
                    field.variant.clone().into(),
                    field.nullable.unwrap_or(true)
                ))
            .collect());

        CsvImportConfig {
            fields,
            path: config.path.clone(),
            sql: config.sql.clone(),
            limit: config.limit,
            header: config.header.unwrap_or(false), 
            delimiter: config.delimiter.unwrap_or(b','),
        }
    }
}

#[derive(Clone, Debug)]
pub struct AvroImportConfig {
    pub path: String,
    pub sql: Option<Sql>,
    pub limit: Option<usize>,
}
impl AvroImportConfig {
    pub fn new(config: &ImportAvro) -> AvroImportConfig
    {
        AvroImportConfig {
            path: config.path.clone(),
            sql: config.sql.clone(),
            limit: config.limit
        }
    }
}

#[derive(Clone, Debug)]
pub struct ParquetImportConfig {
    pub path: String,
    pub sql: Option<Sql>,
    pub limit: Option<usize>,
}
impl ParquetImportConfig {
    pub fn new(config: &ImportParquet) -> ParquetImportConfig
    {
        ParquetImportConfig {
            path: config.path.clone(),
            sql: config.sql.clone(),
            limit: config.limit
        }
    }
}
