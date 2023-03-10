mod actions;
mod config;
mod plans;
mod tool;
mod workflow;

use std::fs;
use std::path::PathBuf;

use clap::Parser;
use plans::Plan;
use workflow::Workflow;

#[derive(Parser)]
#[command(author, version, about)]
struct Args {
    /// Sets the plan to run
    #[arg(short, long, value_name = "PATH")]
    plan: PathBuf,

    /// Turn debugging information on
    #[arg(short, long, action = clap::ArgAction::Count)]
    debug: u8,
}

#[tokio::main]
async fn main()
{
    let args = Args::parse();
    let data = fs::read_to_string(args.plan).unwrap();

    let plan: Plan = serde_json::from_str(&data).unwrap();
    if args.debug > 1 { println!("{:?}", plan); }

    let wf = Workflow::new(&plan);
    if args.debug > 1 { println!("{:?}", wf); }

    wf.run(args.debug).await.unwrap()
}
