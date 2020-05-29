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

    match solr.collections().get("users".into()).await {
        Ok(col) => {
            let schema = col.schema().get().await?;
            println!("Schema: {:?}", schema);
            solr.collections().delete(&col.name).await?;
        },
        Err(_) => {}
    };

    let mut users = solr.collections()
        .create("users".into())
        .router_field("id".into())
        .num_shards(16)
        .max_shards_per_node(16)
        .commit().await?;

    users.schema()
    //     .add_field(FieldBuilder::string("name".into()))
    //     .add_field(FieldBuilder::numeric("age".into()))
        .add_field(FieldBuilder::string("some_shit".into()))
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
        .fl("name,age".into())
        .commit().await?;
    println!("{:#?}", users_found);

    users.schema()
        .delete_field("some_shit".into())
        .commit().await?;

    Ok(())
}
