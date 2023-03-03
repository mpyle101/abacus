use datafusion::dataframe::DataFrame;
use datafusion::error::Result;
use datafusion::execution::context::SessionContext;
use datafusion::execution::options::{AvroReadOptions, CsvReadOptions, ParquetReadOptions};

use crate::config::*;
use crate::{plan, plan::InputSide};

#[derive(Clone, Debug)]
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
            join(conf) => Action::Join(JoinConfig::new(conf)),
            input(format) => match format {
                Input::csv(conf)     => Action::InputCsv(CsvInputConfig::new(&conf.path)),
                Input::avro(conf)    => Action::InputAvro(AvroInputConfig::new(&conf.path)),
                Input::parquet(conf) => Action::InputParquet(ParquetInputConfig::new(&conf.path)),
            },
            output(format) => match format {
                Output::csv(conf)     => Action::OutputCsv(CsvOutputConfig::new(&conf.path)),
                Output::parquet(conf) => Action::OutputParquet(ParquetOutputConfig::new(&conf.path)),
            },
        };

        Tool { id, action }
    }

    pub fn is_ready(&self, data: &ToolData) -> bool
    {
        let needed = self.action.frames();
    
        needed == 0 ||
        needed == 1 && data.left.is_some() ||
        needed == 2 && data.left.is_some() && data.right.is_some()
    }

    pub async fn run(&self, data: Option<ToolData>) -> Result<Option<DataFrame>>
    {
        let ctx = SessionContext::new();
        self.action.run(&ctx, data).await
    }
}

#[derive(Clone, Debug, Default)]
pub struct ToolData {
    left: Option<DataFrame>,
    right: Option<DataFrame>,
}
impl ToolData {
    pub fn set(&mut self, side: InputSide, df: DataFrame)
    {
        match side {
            InputSide::left  => self.left  = Some(df),
            InputSide::right => self.right = Some(df),
        }
    }
}

#[derive(Clone, Debug)]
pub enum Action {
    // Data
    Join(JoinConfig),

    // Input
    InputCsv(CsvInputConfig),
    InputAvro(AvroInputConfig),
    InputParquet(ParquetInputConfig),

    // Output
    OutputCsv(CsvOutputConfig),
    OutputParquet(ParquetOutputConfig),
}

impl Action {
    fn frames(&self) -> i8
    {
        use Action::*;

        match self {
            InputCsv(_) | InputAvro(_) | InputParquet(_) => 0,
            Join(_) => 2,
            _ => 1
        }
    }

    async fn run(&self, ctx: &SessionContext, data: Option<ToolData>) -> Result<Option<DataFrame>>
    {
        use Action::*;

        match self {
            InputCsv(conf) => {
                let df = ctx.read_csv(conf.path.clone(), CsvReadOptions::default()).await?;
                return Ok(Some(df))
            },
            InputAvro(conf) => {
                let df = ctx.read_avro(conf.path.clone(), AvroReadOptions::default()).await?;
                return Ok(Some(df))
            },
            InputParquet(conf) => {
                let df = ctx.read_parquet(conf.path.clone(), ParquetReadOptions::default()).await?;
                return Ok(Some(df))
            },
            _ => {}
        }

        let mut data = data.unwrap();
        match self {
            Join(conf) => {
                let left  = data.left.take().unwrap();
                let right = data.right.take().unwrap();
                let lt_cols = conf.left_cols.iter()
                    .map(|c| c.as_ref())
                    .collect::<Vec<_>>();
                let rt_cols = conf.right_cols.iter()
                    .map(|c| c.as_ref())
                    .collect::<Vec<_>>();
                let frame = left.join(right, conf.join_type, &lt_cols, &rt_cols, None)?;

                Ok(Some(frame))
            },
            OutputCsv(conf) => {
                let plan = data.left.take().unwrap().create_physical_plan().await?;
                ctx.write_csv(plan, conf.path.clone()).await?;
                Ok(None)
            },
            OutputParquet(conf) => {
                let plan = data.left.take().unwrap().create_physical_plan().await?;
                ctx.write_parquet(plan, conf.path.clone(), None).await?;
                Ok(None)
            },
            _ => unreachable!()
        }
    }

}