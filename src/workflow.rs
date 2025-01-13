use std::collections::HashMap;
use std::error::Error;

use datafusion::dataframe::DataFrame;
use datafusion::error::Result;
use datafusion::execution::context::SessionContext;

use petgraph::Direction::Incoming;
use petgraph::graph::{Graph, NodeIndex};
use petgraph::visit::EdgeRef;
use tokio::task::JoinSet;

use crate::plans::{InputSide, Plan};
use crate::tool::{Tool, ToolData};

type WorkflowGraph = Graph<Tool, InputSide>;

#[derive(Debug)]
#[allow(unused)]
pub struct Workflow {
    id: String,
    name: String,
    graph: WorkflowGraph,
}
impl Workflow {
    pub fn new(plan: &Plan) -> Workflow
    {
        let count = plan.tools.len();
        let mut graph  = Graph::<Tool, InputSide>::with_capacity(count, count);

        let nodes = plan.tools.iter()
            .map(|schema| {
                let tool = Tool::new(schema);
                let node = graph.add_node(tool);
                (schema.id(), node)
            })
            .collect::<HashMap<_,_>>();

        plan.links.iter()
            .for_each(|link| {
                let src = nodes.get(&link.src).unwrap();
                let dst = nodes.get(&link.dst).unwrap();
                graph.add_edge(*src, *dst, link.input);
            });

        Workflow { 
            id: plan.id.into(),
            name: plan.name.into(),
            graph,
        }
    }

    pub async fn run(&self, debug: u8) -> Result<(), Box<dyn Error>>
    {
        use std::collections::VecDeque;

        let mut dfs: HashMap<NodeIndex, ToolData> = HashMap::new();
        let mut ready = VecDeque::from_iter(self.graph.externals(Incoming));
        while !ready.is_empty() {
            let mut results = vec![];
            let mut async_tools = JoinSet::new();
            while let Some(ix) = ready.pop_front() {
                if debug > 0 {
                    println!("{:?}", self.graph[ix])
                }
                let data = dfs.remove(&ix);
                if self.graph[ix].is_async() {
                    let ctx  = SessionContext::new();
                    let tool = self.graph[ix].clone();
                    async_tools.spawn(async move { run_async(ix, ctx, tool, data).await });
                } else {
                    let result = self.graph[ix].run_sync(data);
                    if result.is_err() {
                        println!("ERROR [{:?}] {:?}", self.graph[ix].id, result);
                    }
                    results.push((ix, result?))
                }
            }
            while let Some(res) = async_tools.join_next().await {
                results.push(res.unwrap().unwrap());
            }

            for (ix, opt) in results {
                if let Some(df) = opt {
                    self.graph.edges(ix)
                        .map(|edge| (edge.target(), *edge.weight()))
                        .for_each(|(node, side)| {
                            let data = dfs.entry(node).or_default();
                            data.set(side, df.clone());
                            if self.graph[node].is_ready(data) {
                                ready.push_back(node)
                            }
                        });
                }
            }
        }

        Ok(())
    }

}

async fn run_async(
    ix: NodeIndex,
    ctx: SessionContext,
    tool: Tool,
    data: Option<ToolData>
) -> Result<(NodeIndex, Option<DataFrame>)>
{
    let res = tool.run_async(ctx, data).await?;
    Ok((ix, res))
}