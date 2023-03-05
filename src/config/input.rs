use crate::plan::{InputAvro, InputCsv, InputParquet};
use datafusion::arrow::datatypes::Field;

#[derive(Clone, Debug)]
#[allow(unused)]
pub struct CsvInputConfig {
    pub path: String,
    pub limit: Option<usize>,
    pub fields: Option<Vec<Field>>,
    pub header: bool,
    pub delimiter: u8,
}
impl CsvInputConfig {
    pub fn new(conf: &InputCsv) -> CsvInputConfig
    {
        let fields = conf.schema.as_ref().map(|v| v.iter()
            .flat_map(|m| m.iter()
                .map(|(col, dt)| Field::new(col, dt.into(), true)))
            .collect());

        CsvInputConfig {
            fields,
            path: conf.path.clone(),
            limit: conf.limit,
            header: conf.header.unwrap_or(false), 
            delimiter: conf.delimiter.unwrap_or(b','),
        }
    }
}

#[derive(Clone, Debug)]
pub struct AvroInputConfig {
    pub path: String,
    pub limit: Option<usize>,
}
impl AvroInputConfig {
    pub fn new(conf: &InputAvro) -> AvroInputConfig
    {
        AvroInputConfig { path: conf.path.clone(), limit: conf.limit }
    }
}

#[derive(Clone, Debug)]
pub struct ParquetInputConfig {
    pub path: String,
    pub limit: Option<usize>,
}
impl ParquetInputConfig {
    pub fn new(conf: &InputParquet) -> ParquetInputConfig
    {
        ParquetInputConfig { path: conf.path.clone(), limit: conf.limit }
    }
}
