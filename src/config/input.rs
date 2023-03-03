
#[derive(Clone, Debug)]
#[allow(unused)]
pub struct CsvInputConfig {
    pub path: String,
    header: bool,
    delimiter: u8,
}
impl CsvInputConfig {
    pub fn new(path: &str) -> CsvInputConfig
    {
        CsvInputConfig { path: path.to_string(), header: true, delimiter: b',' }
    }
}

#[derive(Clone, Debug)]
pub struct AvroInputConfig {
    pub path: String,
}
impl AvroInputConfig {
    pub fn new(path: &str) -> AvroInputConfig
    {
        AvroInputConfig { path: path.to_string() }
    }
}

#[derive(Clone, Debug)]
pub struct ParquetInputConfig {
    pub path: String,
}
impl ParquetInputConfig {
    pub fn new(path: &str) -> ParquetInputConfig
    {
        ParquetInputConfig { path: path.to_string() }
    }
}