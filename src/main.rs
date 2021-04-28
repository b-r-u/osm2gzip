use std::fs::File;
use std::io::{BufWriter, Write};
use std::sync::{Arc, Mutex};

use flate2::write::GzEncoder;
use flate2::Compression;
use osmpbf::{BlobDecode, BlobReader, DenseNode, Element, Node, Relation, Way};
use rayon::prelude::*;

#[derive(Debug)]
struct ElementSink {
    writer: GzEncoder<BufWriter<File>>,
    num_elements: u64,
    filenum: Arc<Mutex<u64>>,
}

impl ElementSink {
    const MAX_ELEMENTS_COUNT: u64 = 100_000;

    fn new(filenum: Arc<Mutex<u64>>) -> Result<Self, std::io::Error> {
        let f = File::create(Self::new_file_path(&filenum))?;
        let writer = GzEncoder::new(BufWriter::new(f), Compression::fast());

        Ok(ElementSink {
            writer,
            num_elements: 0,
            filenum,
        })
    }

    fn increment_and_cycle(&mut self) -> Result<(), std::io::Error> {
        self.num_elements += 1;
        if self.num_elements >= Self::MAX_ELEMENTS_COUNT {
            let f = File::create(Self::new_file_path(&self.filenum))?;
            let mut writer = GzEncoder::new(BufWriter::new(f), Compression::fast());
            std::mem::swap(&mut writer, &mut self.writer);
            writer.finish()?.flush()?;
            self.num_elements = 0;
        }
        Ok(())
    }

    fn new_file_path(filenum: &Arc<Mutex<u64>>) -> String {
        let mut num = filenum.lock().unwrap();
        let path = format!("elements_{:05}.txt.gz", num);
        *num += 1;
        path
    }

    fn add_node(&mut self, node: &Node) -> Result<(), std::io::Error> {
        writeln!(self.writer, "node {}", node.id())?;
        self.increment_and_cycle()
    }

    fn add_dense_node(&mut self, node: &DenseNode) -> Result<(), std::io::Error> {
        writeln!(self.writer, "node {}", node.id())?;
        self.increment_and_cycle()
    }

    fn add_way(&mut self, way: &Way) -> Result<(), std::io::Error> {
        writeln!(self.writer, "way {}", way.id())?;
        self.increment_and_cycle()
    }

    fn add_relation(&mut self, relation: &Relation) -> Result<(), std::io::Error> {
        writeln!(self.writer, "relation {}", relation.id())?;
        self.increment_and_cycle()
    }
}

fn main() -> anyhow::Result<()> {
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 2 {
        println!("Need *.osm.pbf file as first argument.");
        return Ok(());
    }
    let reader = BlobReader::from_path(&args[1])?;

    let sinkpool: Arc<Mutex<Vec<ElementSink>>> = Arc::new(Mutex::new(vec![]));
    let filenum: Arc<Mutex<u64>> = Arc::new(Mutex::new(0));

    let get_sink_from_pool = || -> Result<ElementSink, std::io::Error> {
        {
            let mut pool = sinkpool.lock().unwrap();
            if let Some(sink) = pool.pop() {
                return Ok(sink);
            }
        }
        ElementSink::new(filenum.clone())
    };

    let add_sink_to_pool = |sink| {
        let mut pool = sinkpool.lock().unwrap();
        pool.push(sink);
    };

    reader
        .par_bridge()
        .try_for_each(|blob| -> anyhow::Result<()> {
            if let BlobDecode::OsmData(block) = blob?.decode()? {
                let mut sink = get_sink_from_pool()?;
                for elem in block.elements() {
                    match elem {
                        Element::Node(ref node) => {
                            sink.add_node(node)?;
                        }
                        Element::DenseNode(ref node) => {
                            sink.add_dense_node(node)?;
                        }
                        Element::Way(ref way) => {
                            sink.add_way(way)?;
                        }
                        Element::Relation(ref rel) => {
                            sink.add_relation(rel)?;
                        }
                    }
                }
                add_sink_to_pool(sink);
            }
            Ok(())
        })?;

    {
        let mut pool = sinkpool.lock().unwrap();
        for sink in pool.drain(..) {
            sink.writer.finish()?.flush()?;
        }
    }
    Ok(())
}
