
#[derive(Clone, Debug)]
pub struct CsvOutputConfig {
    pub path: String,
    pub overwrite: bool,
}
impl CsvOutputConfig {
    pub fn new(path: &str, overwrite: bool) -> CsvOutputConfig
    {
        CsvOutputConfig { path: path.to_string(), overwrite  }
    }
}

#[derive(Clone, Debug)]
pub struct ParquetOutputConfig {
    pub path: String,
    pub overwrite: bool,
}
impl ParquetOutputConfig {
    pub fn new(path: &str, overwrite: bool) -> ParquetOutputConfig
    {
        ParquetOutputConfig { path: path.to_string(), overwrite }
    }
}