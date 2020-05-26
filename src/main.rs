extern crate solrdrv;

use solrdrv::{
    serde_json::json,
    tokio,
    Solr,
    FieldBuilder
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let solr = Solr::client("http".into(), "localhost".into(), 8983);

    let _res = match solr.collections().get("users".into()).await {
        Ok(col) => solr.collections().delete(&col.name).await,
        Err(_) => Ok(())
    };

    let mut users = solr.collections()
        .create("users".into())
        .router_field("id".into())
        .num_shards(16)
        .max_shards_per_node(16)
        .field(FieldBuilder::string("name".into()))
        .field(FieldBuilder::numeric("age".into()))
        .commit().await?;

    users.add(json!([
        {
            "name": "Some",
            "age": 19
        },
        {
            "name": "Dude",
            "age": 21
        }
    ]));

    if users.get_commit_size() > 0 {
        users.commit().await?;
    }

    let users_found = users.search()
        .query("(name:Some AND age:19) OR age:21".into())
        // .from_json(json!(...)) // TODO: Add JSON -> query string method!
        .sort("name asc".into())
        .fields("name,age".into())
        .commit().await?;
    println!("{:#?}", users_found);

    Ok(())
}
