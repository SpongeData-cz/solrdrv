extern crate solrdrv;

use solrdrv::tokio;
use solrdrv::SolrDrv;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let drv = SolrDrv::new(
        String::from("http"),
        String::from("localhost"),
        8983);

    let info = match drv.get_system_info().await {
        Ok(r) => r,
        Err(e) => return Err(e.into()),
    };

    println!("Info: {:?}", info);

    let collections = match drv.list_collections().await {
        Ok(r) => r,
        Err(e) => return Err(e.into()),
    };

    for c in &collections {
        println!("{:?}", c);
    }

    let entities = match drv.get_collection(&String::from("entities")).await {
        Ok(r) => r,
        Err(e) => return Err(e.into()),
    };

    let documents = match entities.select(&String::from("*:*&rows=1")).await {
        Ok(r) => r,
        Err(e) => return Err(e.into()),
    };

    for d in &documents {
        println!("{:?}", d);
    }

    Ok(())
}
