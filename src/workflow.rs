use std::collections::HashMap;
use std::error::Error;

use datafusion::dataframe::DataFrame;
use datafusion::error::Result;
use datafusion::execution::context::SessionContext;

use petgraph::visit::EdgeRef;
use petgraph::graph::{Graph, NodeIndex};
use tokio::task::JoinSet;

use crate::plan::{self, InputSide, Plan};
use crate::tool::{Tool, ToolData};

type WorkflowGraph = Graph<Tool, InputSide>;

#[derive(Debug)]
#[allow(unused)]
pub struct Workflow {
    id: String,
    name: String,
    tools: WorkflowGraph,
    inputs: Vec<NodeIndex>,
}

impl Workflow {
    pub fn new(plan: &Plan) -> Workflow
    {
        let count = plan.tools.len();
        let mut tools  = Graph::<Tool, InputSide>::with_capacity(count, count);
        let mut inputs = vec![];

        let nodes = plan.tools.iter()
            .map(|schema| {
                let tool = Tool::new(schema);
                let node = tools.add_node(tool);
                if let plan::Tool::input(..) = schema {
                    inputs.push(node);
                }

                (schema.id(), node)
            })
            .collect::<HashMap<_,_>>();

        plan.links.iter()
            .for_each(|link| {
                let src = nodes.get(&link.src).unwrap();
                let dst = nodes.get(&link.dst).unwrap();
                tools.add_edge(*src, *dst, link.input);
            });

        Workflow { 
            id: plan.id.clone(),
            name: plan.name.clone(),
            tools,
            inputs,
        }
    }

    pub async fn run(&self) -> Result<(), Box<dyn Error>>
    {
        use std::collections::VecDeque;

        let mut dfs: HashMap<NodeIndex, ToolData> = HashMap::new();
        let mut ready = VecDeque::from_iter(self.inputs.iter().cloned());
        while !ready.is_empty() {
            println!("{:?}", ready);

            let mut results = vec![];
            let mut async_tools = JoinSet::new();
            while let Some(ix) = ready.pop_front() {
                let data = dfs.remove(&ix);
                if self.tools[ix].is_async() {
                    let ctx = SessionContext::new();
                    async_tools.spawn(run_async(ix, ctx, self.tools[ix].clone(), data));
                } else {
                    results.push((ix, self.tools[ix].run_sync(data)?))
                }
            }
            while let Some(res) = async_tools.join_next().await {
                results.push(res.unwrap().unwrap());
            }

            for (ix, opt) in results {
                if let Some(df) = opt {
                    self.tools.edges(ix)
                        .map(|edge| (edge.target(), *edge.weight()))
                        .for_each(|(node, side)| {
                            let data = dfs.entry(node).or_default();
                            data.set(side, df.clone());
                            if self.tools[node].is_ready(data) {
                                ready.push_back(node)
                            }
                        });
                }
            }
        }

        Ok(())
    }

}

async fn run_async(ix: NodeIndex, ctx: SessionContext, tool: Tool, data: Option<ToolData>) -> Result<(NodeIndex, Option<DataFrame>)>
{
    let res = tool.run_async(ctx, data).await?;
    Ok((ix, res))
}