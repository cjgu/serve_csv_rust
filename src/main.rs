#![feature(plugin)]
#![plugin(rocket_codegen)]

extern crate csv;
extern crate byteorder;
extern crate time;
extern crate rocket;
extern crate serde_json;
extern crate rocket_contrib;
#[macro_use] extern crate serde_derive;

use std::io::{self, Write};
use csv::index::{Indexed, create_index};

use csv::{Reader, Result};

use std::collections::BTreeMap;

use std::sync::Mutex;
use rocket::{Rocket, State};
use rocket_contrib::Json;

type IndexCon = Mutex<CsvIndex>;

fn create_btree_index<R>(mut rdr: Reader<R>, btree: &mut BTreeMap<u64, u64>) -> Result<()>
         where R: io::Read + io::Seek {
    // Seek to the beginning so that we get everything.
    try!(rdr.seek(0));

    let mut count = 0u64;
    for record in rdr.decode() {
        let (id_str, _): (String, String) = record.unwrap();
        let id = id_str.parse::<u64>().expect("Non-integer in first column");
        btree.insert(id, count);
        count += 1;
    }
    Ok(())
}

fn usage() {
    println!("Usage: serve_csv <csv-file>");
    std::process::exit(-1);
}

fn build_btree_index(csv_file: &str) -> BTreeMap<u64, u64> {
    let pre_btree = time::precise_time_ns();

    let rdr = csv::Reader::from_file(csv_file).expect("Cant read file").has_headers(false);

    let mut id_to_row_index: BTreeMap<u64, u64> = BTreeMap::new();
    create_btree_index(rdr, &mut id_to_row_index).unwrap();

    let post_btree = time::precise_time_ns();
    println!("Btree time: {:?} ms", (post_btree-pre_btree)/1000_000);

    id_to_row_index
}

fn build_offset_index(csv_file: &str) -> Indexed<std::fs::File, io::Cursor<Vec<u8>>>  {
    let pre_index = time::precise_time_ns();

    let rdr = || csv::Reader::from_file(csv_file).expect("Cant read file").has_headers(false);
    let mut offset_index_data = io::Cursor::new(Vec::new());

    create_index(rdr(), offset_index_data.by_ref()).unwrap();

    let index = Indexed::open(rdr(), offset_index_data).unwrap();

    let post_index = time::precise_time_ns();
    println!("Offset index time: {:?} ms", (post_index-pre_index)/1000_000);

    index
}

struct CsvIndex {
    id_to_row: BTreeMap<u64, u64>,
    offset: Indexed<std::fs::File, io::Cursor<Vec<u8>>>
}

impl CsvIndex {
    fn lookup(&mut self, id: u64) -> Option<Vec<String>> {
        let pre_btree_lookup = time::precise_time_ns();
        let row_index_result = self.id_to_row.get(&id);
        let post_btree_lookup = time::precise_time_ns();

        if let Some(row_index) = row_index_result {
            println!("Row index: {:?}", *row_index);
            println!("Btree lookup time: {:?} us", (post_btree_lookup-pre_btree_lookup)/1000);

            let pre_lookup = time::precise_time_ns();

            self.offset.seek(*row_index).unwrap();

            let row = self.offset.records().next().unwrap().unwrap();
            let post_lookup = time::precise_time_ns();

            println!("Lookup time: {:?} us", (post_lookup-pre_lookup)/1000);

            Some(row)
        }
        else {
            return None;
        }
    }
}

#[derive(Serialize)]
struct LookupResponse {
    status: u64,
    recommendations: Vec<Recommendation>
}

#[derive(Serialize)]
struct Recommendation {
    item_id: u64,
    score: f64
}

#[get("/<id>")]
fn lookup(id: u64, index_con: State<IndexCon>) -> Json<LookupResponse>  {
    let row = index_con.lock()
        .expect("index connection lock")
        .lookup(id);

    let pre = time::precise_time_ns();

    if let Some(content) = row {
        let mut recs = Vec::new();
        let mut idx = 0;
        for col in content {
            if idx < 1 {
                idx += 1;
                continue;
            }
            let parts: Vec<&str> = col.split(":").collect();

            let item_id = parts[0].parse::<u64>().unwrap();
            let score = parts[1].parse::<f64>().unwrap();

            recs.push(Recommendation{
                item_id: item_id,
                score: score
            })
        }

        let post = time::precise_time_ns();
        println!("Post processing: {:?} us", (post-pre)/1000);
        Json(LookupResponse {
            recommendations: recs,
            status: 200 
        })
    }
    else {
        let post = time::precise_time_ns();
        println!("Post processing: {:?} us", (post-pre)/1000);
        Json(LookupResponse {
            recommendations: vec![],
            status: 404
        })
    }

}

fn rocket(csv_file: &str) -> Rocket {
    let id_to_row_index = build_btree_index(csv_file);
    let offset_index = build_offset_index(csv_file);

    let index = CsvIndex{id_to_row: id_to_row_index, offset: offset_index};

    rocket::ignite()
        .manage(Mutex::new(index))
        .mount("/", routes![lookup])
}

fn main() {
    let args: Vec<String> = std::env::args().collect();

    if args.len() != 2 {
        usage();
    }

    let csv_file = &args[1];

    rocket(&csv_file).launch();
}
