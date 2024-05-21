use std::env::args;
use std::process::exit;

fn main() {
    let args: Vec<String> = args().into_iter().collect();
    if args.len() != 2 {
        println!("Usage: {} config_dir", args[0]);
        exit(1);
    }
    println!("Application Starting");
}
