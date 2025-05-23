use std::fs::create_dir_all;
use crate::db::vector::Vector;
use crate::db::lsm::LSMTree;
use std::path::Path;
mod db;

fn main() {
    println!("Creating wal dir");
    create_dir_all("./wal").unwrap();

    let mut lsm = LSMTree::new(Path::new("./data")).unwrap();
    lsm.insert(1, Vector::new(1, vec![0.0, 1.1, 2.2]));
}
