use crate::plan::{InputAvro, InputCsv, InputParquet, Sql};
use datafusion::arrow::datatypes::Field;

#[derive(Clone, Debug)]
#[allow(unused)]
pub struct CsvInputConfig {
    pub path: String,
    pub header: bool,
    pub delimiter: u8,
    pub sql: Option<Sql>,
    pub limit: Option<usize>,
    pub fields: Option<Vec<Field>>,
}
impl CsvInputConfig {
    pub fn new(config: &InputCsv) -> CsvInputConfig
    {
        let fields = config.schema.as_ref().map(|v| v.iter()
            .map(|field| 
                Field::new(
                    field.column.clone(),
                    field.variant.clone().into(),
                    field.nullable.unwrap_or(true)
                ))
            .collect());

        CsvInputConfig {
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
pub struct AvroInputConfig {
    pub path: String,
    pub sql: Option<Sql>,
    pub limit: Option<usize>,
}
impl AvroInputConfig {
    pub fn new(config: &InputAvro) -> AvroInputConfig
    {
        AvroInputConfig {
            path: config.path.clone(),
            sql: config.sql.clone(),
            limit: config.limit
        }
    }
}

#[derive(Clone, Debug)]
pub struct ParquetInputConfig {
    pub path: String,
    pub sql: Option<Sql>,
    pub limit: Option<usize>,
}
impl ParquetInputConfig {
    pub fn new(config: &InputParquet) -> ParquetInputConfig
    {
        ParquetInputConfig {
            path: config.path.clone(),
            sql: config.sql.clone(),
            limit: config.limit
        }
    }
}
