use super::common;
use arrow::record_batch::RecordBatch;
use datafusion::error::DataFusionError;
use parquet::{arrow::arrow_writer::ArrowWriter, file::properties::WriterProperties};
use std::fs;
use std::path::Path;
// use tpchgen::generators::{
//     CustomerGenerator, LineItemGenerator, NationGenerator, OrderGenerator, PartGenerator,
//     PartSuppGenerator, RegionGenerator, SupplierGenerator,
// };
// use tpchgen_arrow::{
//     CustomerArrow, LineItemArrow, NationArrow, OrderArrow, PartArrow, PartSuppArrow, RegionArrow,
//     SupplierArrow,
// };

pub fn get_queries() -> Vec<String> {
    common::get_queries("testdata/tpch/queries")
}

pub fn get_query(id: &str) -> Result<String, DataFusionError> {
    common::get_query("testdata/tpch/queries", id)
}

fn generate_table<A>(
    mut data_source: A,
    table_name: &str,
    data_dir: &Path,
) -> Result<(), Box<dyn std::error::Error>>
where
    A: Iterator<Item = RecordBatch>,
{
    let output_path = data_dir.join(format!("{table_name}.parquet"));

    if let Some(first_batch) = data_source.next() {
        let file = fs::File::create(&output_path)?;
        let props = WriterProperties::builder().build();
        let mut writer = ArrowWriter::try_new(file, first_batch.schema(), Some(props))?;

        writer.write(&first_batch)?;

        for batch in data_source {
            writer.write(&batch)?;
        }

        writer.close()?;
    }

    Ok(())
}

/// Generates all TPC-H tables as parquet files in the specified data directory.
pub fn generate_tpch_data(
    data_dir: &Path,
    sf: f64,
    parts: usize,
) -> Result<(), Box<dyn std::error::Error>> {
    unimplemented!("Dataset generation is temporarily disabled.");
}
