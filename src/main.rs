extern crate solrdrv;

use solrdrv::tokio;
use solrdrv::SolrDrv;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let drv = SolrDrv::new(
        "http".into(),
        "localhost".into(),
        8983);

    // let info = match drv.get_system_info().await {
    //     Ok(r) => r,
    //     Err(e) => return Err(e.into()),
    // };
    // println!("Info: {:?}", info);

    let collections = match drv.list_collections().await {
        Ok(r) => r,
        Err(e) => return Err(e.into()),
    };
    for c in &collections {
        println!("{:?}", c);
    }

    // let entities = match drv.get_collection(&"entities".into()).await {
    //     Ok(r) => r,
    //     Err(e) => return Err(e.into()),
    // };
    //
    // let documents = match entities.select(&"*:*&rows=1".into()).await {
    //     Ok(r) => r,
    //     Err(e) => return Err(e.into()),
    // };
    // for d in &documents {
    //     println!("{:?}", d);
    // }

    let test = match drv.create_collection(&"test".into()).await {
        Ok(r) => r,
        Err(e) => return Err(e.into()),
    };
    println!("Created collection {:?}", test);

    match drv.delete_collection(&test.name).await {
        Ok(r) => r,
        Err(e) => return Err(e.into()),
    };
    println!("Deleted collection test");

    Ok(())
}
