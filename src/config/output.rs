
#[derive(Clone, Debug)]
pub struct CsvOutputConfig {
    pub path: String,
}
impl CsvOutputConfig {
    pub fn new(path: &str) -> CsvOutputConfig
    {
        CsvOutputConfig { path: path.to_string() }
    }
}

#[derive(Clone, Debug)]
pub struct ParquetOutputConfig {
    pub path: String,
}
impl ParquetOutputConfig {
    pub fn new(path: &str) -> ParquetOutputConfig
    {
        ParquetOutputConfig { path: path.to_string() }
    }
}