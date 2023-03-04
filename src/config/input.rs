use crate::plan::{InputAvro, InputCsv, InputParquet};

#[derive(Clone, Debug)]
#[allow(unused)]
pub struct CsvInputConfig {
    pub path: String,
    pub limit: Option<usize>,
    header: bool,
    delimiter: u8,
}
impl CsvInputConfig {
    pub fn new(conf: &InputCsv) -> CsvInputConfig
    {
        CsvInputConfig {
            path: conf.path.clone(),
            limit: conf.limit,
            header: true, 
            delimiter: b','
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