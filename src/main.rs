mod plan;
mod config;
mod tool;
mod workflow;

use std::error::Error;
use std::fs::File;
use std::io::BufReader;
use std::path::{Path, PathBuf};

use clap::Parser;
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

    let plan = read_plan(args.plan).unwrap();
    if args.debug > 0 { println!("{:?}", plan); }

    let wf = Workflow::new(&plan);
    if args.debug > 0 { println!("{:?}", wf); }

    match wf.run().await {
        Ok(_) => {},
        Err(e) => println!("{:?}", e)
    }
}

fn read_plan<P>(path: P) -> Result<plan::Plan, Box<dyn Error>>
    where P: AsRef<Path>
{
    let file = File::open(path)?;
    let reader = BufReader::new(file);
    let plan = serde_json::from_reader(reader)?;

    Ok(plan)
}