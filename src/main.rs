extern crate csv;
extern crate byteorder;
extern crate time;


use byteorder::{WriteBytesExt, BigEndian};

use std::io::{self, Write};
use csv::index::{Indexed};

use csv::{Reader, NextField, Result};

use std::collections::BTreeMap;

fn create_index<R, W>(mut rdr: Reader<R>, mut wtr: W) -> Result<()>
        where R: io::Read + io::Seek, W: io::Write {
    // Seek to the beginning so that we get everything.
    try!(rdr.seek(0));
    let mut count = 0u64;
    while !rdr.done() {
        // wtr.insert()
        try!(wtr.write_u64::<BigEndian>(rdr.byte_offset()));
        loop {
            match rdr.next_bytes() {
                NextField::EndOfCsv => break,
                NextField::EndOfRecord => { count += 1; break; },
                NextField::Error(err) => return Err(err),
                NextField::Data(_) => {}
            }
        }
    }
    wtr.write_u64::<BigEndian>(count).map_err(From::from)
}

fn create_btree_index<R>(mut rdr: Reader<R>, btree: &mut BTreeMap<u64, u64>) -> Result<()>
         where R: io::Read + io::Seek {
    // Seek to the beginning so that we get everything.
    try!(rdr.seek(0));

    let mut count = 0u64;
    let mut skip = true;
    for record in rdr.decode() {
        if skip {
            // Skip header
            skip = false;
            continue;
        }
        let (id_str, _): (String, String) = record.unwrap();
        let id = id_str.parse::<u64>().expect("Non-integer in first column");
        btree.insert(id, count);
        count += 1;
    }
    Ok(())
}

fn usage() {
    println!("Usage: cvslookup <csv-file> <lookup-id>");
    std::process::exit(-1);
}

fn build_btree_index(csv_file: &str) -> BTreeMap<u64, u64> {
    let pre_btree = time::precise_time_ns();

    let rdr = csv::Reader::from_file(csv_file).expect("Cant read file");


    let mut id_to_row_index: BTreeMap<u64, u64> = BTreeMap::new();
    create_btree_index(rdr, &mut id_to_row_index).unwrap();

    let post_btree = time::precise_time_ns();
    println!("Btree time: {:?} us", (post_btree-pre_btree)/1000);

    id_to_row_index
}

fn build_offset_index(csv_file: &str) -> Indexed<std::fs::File, io::Cursor<Vec<u8>>>  {
    let pre_index = time::precise_time_ns();

    let rdr = || csv::Reader::from_file(csv_file).expect("Cant read file");
    let mut offset_index_data = io::Cursor::new(Vec::new());

    create_index(rdr(), offset_index_data.by_ref()).unwrap();

    let index = Indexed::open(rdr(), offset_index_data).unwrap();

    let post_index = time::precise_time_ns();
    println!("Offset index time: {:?} us", (post_index-pre_index)/1000);

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
            println!("Btree lookup time: {:?} ns", post_btree_lookup-pre_btree_lookup);

            let pre_lookup = time::precise_time_ns();
            // Seek to the second record and read its data. This is done *without*
            // reading the first record.
            self.offset.seek(*row_index).unwrap();

            // Read the first row at this position (which is the second record).
            // Since `Indexed` derefs to a `csv::Reader`, we can call CSV reader methods
            // on it directly.
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

fn main() {
    let args: Vec<String> = std::env::args().collect();

    if args.len() != 3 {
        usage();
    }

    let csv_file = &args[1];

    let id_to_row_index = build_btree_index(&csv_file);
    let offset_index = build_offset_index(&csv_file);

    let mut index = CsvIndex{id_to_row: id_to_row_index, offset: offset_index};

    let id_to_find = &args[2].parse::<u64>().expect("Lookup id must be an integer");

    let row = index.lookup(*id_to_find);
    println!("Row: {:?}", row);
}
