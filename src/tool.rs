use datafusion::error::Result;
use datafusion::execution::context::SessionContext;
use datafusion::prelude::DataFrame;

use crate::actions::*;
use crate::config::*;
use crate::plans;

pub use crate::actions::Data as ToolData;

#[derive(Clone, Debug)]
#[allow(unused)]
pub struct Tool {
    pub id: String,
    action: Action,
}
impl Tool {
    pub fn new(plan: &plans::Tool) -> Tool
    {
        use plans::{Import, Export, Tool::*};

        let id = plan.id();
        let action = match plan {
            difference(_)     => Action::Difference,
            distinct(_)       => Action::Distinct,
            intersect(_)      => Action::Intersect,
            filter(config)    => Action::Filter(config.into()),
            join(config)      => Action::Join(config.into()),
            map(config)       => Action::Map(config.into()),
            select(config)    => Action::Select(config.into()),
            sort(config)      => Action::Sort(config.into()),
            summarize(config) => Action::Summarize(config.into()),
            union(config)     => Action::Union(config.into()),

            import(format) => match format {
                Import::csv(config)     => Action::ImportCsv(config.into()),
                Import::avro(config)    => Action::ImportAvro(config.into()),
                Import::parquet(config) => Action::ImportParquet(config.into()),
            },
            export(format) => match format {
                Export::csv(config)     => Action::ExportCsv(config.into()),
                Export::json(config)    => Action::ExportJson(config.into()),
                Export::parquet(config) => Action::ExportParquet(config.into()),
            },
        };

        Tool { action, id: id.into() }
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
    Difference,
    Distinct,
    Intersect,
    Filter(FilterConfig),
    Join(JoinConfig),
    Map(MapConfig),
    Select(SelectConfig),
    Sort(SortConfig),
    Summarize(SummarizeConfig),
    Union(UnionConfig),

    // Import
    ImportCsv(CsvImportConfig),
    ImportAvro(AvroImportConfig),
    ImportParquet(ParquetImportConfig),

    // Export
    ExportCsv(CsvExportConfig),
    ExportJson(JsonExportConfig),
    ExportParquet(ParquetExportConfig),
}
impl Action {
    fn frames(&self) -> i8
    {
        use Action::*;

        match self {
            Distinct | Filter(_) | Map(_) | Select(_)
                | Sort(_) | Summarize(_) => 1,
            Difference | Intersect | Join(_) | Union(_) => 2,
            ImportCsv(_) | ImportAvro(_) | ImportParquet(_) => 0,
            ExportCsv(_) | ExportJson(_) | ExportParquet(_) => 1,
        }
    }

    fn is_async(&self) -> bool
    {
        use Action::*;

        match self {
            Difference | Distinct | Intersect
                | Filter(_) | Join(_) | Map(_)
                | Select(_) | Sort(_) | Summarize(_) | Union(_) => false,
            ImportCsv(_) | ImportAvro(_) | ImportParquet(_) => true,
            ExportCsv(_) | ExportJson(_) | ExportParquet(_) => true
        }
    }

    async fn run_async(&self, ctx: SessionContext, data: Option<ToolData>) -> Result<Option<DataFrame>>
    {
        use Action::*;

        let mut data = data.unwrap_or_default();
        match self {
            ImportCsv(config)     => read_csv(ctx, config).await,
            ImportAvro(config)    => read_avro(ctx, config).await,
            ImportParquet(config) => read_parquet(ctx, config).await,
            ExportCsv(config)     => write_csv(&mut data, config).await,
            ExportJson(config)    => write_json(&mut data, config).await,
            ExportParquet(config) => write_parquet(&mut data, config).await,
            _ => panic!("Sync tool running async")
        }
    }

    fn run_sync(&self, data: Option<ToolData>) -> Result<Option<DataFrame>>
    {
        use Action::*;

        let mut data = data.unwrap();
        match self {
            Difference        => difference(&mut data),
            Distinct          => distinct(&mut data),
            Intersect         => intersect(&mut data),
            Filter(config)    => filter(&mut data, config),
            Join(config)      => join(&mut data, config),
            Map(config)       => project(&mut data, config),
            Select(config)    => select(&mut data, config),
            Sort(config)      => sort(&mut data, config),
            Summarize(config) => summarize(&mut data, config),
            Union(config)     => union(&mut data, config),
            _ => panic!("Async tool running sync")
        }
    }

}