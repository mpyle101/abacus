use std::fs;

use datafusion::arrow::datatypes::Schema;
use datafusion::error::Result;
use datafusion::execution::context::SessionContext;
use datafusion::execution::options::{AvroReadOptions, CsvReadOptions, ParquetReadOptions};
use datafusion::prelude::{col, DataFrame};

use crate::config::*;
use crate::{plan, plan::InputSide};

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
            Join(_) | Union(_) => 2,
            _ => 1
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

        match self {
            InputCsv(conf) => {
                let mut df = if let Some(fields) = conf.fields.as_ref() {
                    let schema = Schema::new(fields.clone());
                    let options = CsvReadOptions {
                        schema: Some(&schema),
                        delimiter: conf.delimiter,
                        has_header: conf.header,
                        ..Default::default()
                    };
                    ctx.read_csv(&conf.path, options).await?
                } else {
                    let options = CsvReadOptions {
                        delimiter: conf.delimiter,
                        has_header: conf.header,
                        ..Default::default()
                    };
                    ctx.read_csv(&conf.path, options).await?
                };
                df = df.limit(0, conf.limit)?;
                return Ok(Some(df))
            },
            InputAvro(conf) => {
                let mut df = ctx.read_avro(&conf.path, AvroReadOptions::default()).await?;
                df = df.limit(0, conf.limit)?;
                return Ok(Some(df))
            },
            InputParquet(conf) => {
                let mut df = ctx.read_parquet(&conf.path, ParquetReadOptions::default()).await?;
                df = df.limit(0, conf.limit)?;
                return Ok(Some(df))
            },
            _ => {}
        }

        let mut data = data.unwrap();
        match self {
            OutputCsv(conf) => {
                if conf.overwrite {
                    let _ = fs::remove_dir_all(&conf.path);
                }
                let plan = data.left.take().unwrap().create_physical_plan().await?;
                ctx.write_csv(plan, &conf.path).await?;
                Ok(None)
            },
            OutputJson(conf) => {
                if conf.overwrite {
                    let _ = fs::remove_dir_all(&conf.path);
                }
                let plan = data.left.take().unwrap().create_physical_plan().await?;
                ctx.write_json(plan, &conf.path).await?;
                Ok(None)
            },
            OutputParquet(conf) => {
                if conf.overwrite {
                    let _ = fs::remove_dir_all(&conf.path);
                }
                let plan = data.left.take().unwrap().create_physical_plan().await?;
                ctx.write_parquet(plan, &conf.path, None).await?;
                Ok(None)
            },
            _ => panic!("Sync tool running async")
        }
    }

    fn run_sync(&self, data: Option<ToolData>) -> Result<Option<DataFrame>>
    {
        use Action::*;

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
            Select(conf) => {
                let df = data.left.take().unwrap();
                let exprs = conf.columns.iter()
                    .map(|c| if let Some(alias) = conf.aliases.get(c) {
                            col(c).alias(alias)
                        } else {
                            col(c)
                        }
                    )
                    .collect::<Vec<_>>();
                let frame = df.select(exprs)?;
                Ok(Some(frame))
            },
            Union(conf) => {
                let df = data.left.take().unwrap();
                let other = data.right.take().unwrap();
                let frame = if conf.distinct {
                    df.union_distinct(other)?
                } else {
                    df.union(other)?
                };
                Ok(Some(frame))
            }
            _ => panic!("Async tool running sync")
        }
    }

}