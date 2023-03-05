use datafusion::error::Result;
use datafusion::execution::context::SessionContext;
use datafusion::prelude::DataFrame;

use crate::actions::*;
use crate::config::*;
use crate::plan;

pub use crate::actions::Data as ToolData;

#[derive(Clone, Debug)]
#[allow(unused)]
pub struct Tool {
    id: String,
    action: Action,
}

impl Tool {
    pub fn new(plan: &plan::Tool) -> Tool
    {
        use plan::{Input, Output, Tool::*};

        let id = plan.id();
        let action = match plan {
            union(conf)   => Action::Union(UnionConfig::new(conf)),
            join(conf)    => Action::Join(JoinConfig::new(conf)),
            select(conf)  => Action::Select(SelectConfig::new(conf)),
            input(format) => match format {
                Input::csv(conf)     => Action::InputCsv(CsvInputConfig::new(conf)),
                Input::avro(conf)    => Action::InputAvro(AvroInputConfig::new(conf)),
                Input::parquet(conf) => Action::InputParquet(ParquetInputConfig::new(conf)),
            },
            output(format) => match format {
                Output::csv(conf)     => Action::OutputCsv(CsvOutputConfig::new(conf)),
                Output::json(conf)    => Action::OutputJson(JsonOutputConfig::new(conf)),
                Output::parquet(conf) => Action::OutputParquet(ParquetOutputConfig::new(conf)),
            },
        };

        Tool { id, action }
    }

    pub fn is_async(&self) -> bool
    {
        self.action.is_async()
    }

    pub fn is_ready(&self, data: &ToolData) -> bool
    {
        let needed = self.action.frames();
    
        needed == 0 ||
        needed == 1 && data.left.is_some() ||
        needed == 2 && data.left.is_some() && data.right.is_some()
    }

    pub fn run_sync(&self, data: Option<ToolData>) -> Result<Option<DataFrame>>
    {
        self.action.run_sync(data)
    }

    pub async fn run_async(&self, ctx: SessionContext, data: Option<ToolData>) -> Result<Option<DataFrame>>
    {
        self.action.run_async(ctx, data).await
    }
}

#[derive(Clone, Debug)]
pub enum Action {
    // Data
    Join(JoinConfig),
    Select(SelectConfig),
    Union(UnionConfig),

    // Input
    InputCsv(CsvInputConfig),
    InputAvro(AvroInputConfig),
    InputParquet(ParquetInputConfig),

    // Output
    OutputCsv(CsvOutputConfig),
    OutputJson(JsonOutputConfig),
    OutputParquet(ParquetOutputConfig),
}

impl Action {
    fn frames(&self) -> i8
    {
        use Action::*;

        match self {
            InputCsv(_) | InputAvro(_) | InputParquet(_) => 0,
            OutputCsv(_) | OutputJson(_) | OutputParquet(_) => 1,
            Select(_) => 1,
            Join(_) | Union(_) => 2,
        }
    }

    fn is_async(&self) -> bool
    {
        use Action::*;

        match self {
            Join(_) | Select(_) | Union(_) => false,
            InputCsv(_) | InputAvro(_) | InputParquet(_) => true,
            OutputCsv(_) | OutputJson(_) | OutputParquet(_) => true
        }
    }

    async fn run_async(&self, ctx: SessionContext, data: Option<ToolData>) -> Result<Option<DataFrame>>
    {
        use Action::*;

        let mut data = data.unwrap_or_default();
        match self {
            InputCsv(config)      => return read_csv(ctx, config).await,
            InputAvro(config)     => return read_avro(ctx, config).await,
            InputParquet(config)  => return read_parquet(ctx, config).await,
            OutputCsv(config)     => write_csv(&mut data, ctx, config).await,
            OutputJson(config)    => write_json(&mut data, ctx, config).await,
            OutputParquet(config) => write_parquet(&mut data, ctx, config).await,
            _ => panic!("Sync tool running async")
        }
    }

    fn run_sync(&self, data: Option<ToolData>) -> Result<Option<DataFrame>>
    {
        use Action::*;

        let mut data = data.unwrap();
        match self {
            Join(config)   => join(&mut data, config),
            Select(config) => select(&mut data, config),
            Union(config)  => union(&mut data, config),
            _ => panic!("Async tool running sync")
        }
    }

}