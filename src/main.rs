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
            col.schema()
                .delete_field("name")
                .delete_field("age")
                .commit().await?;
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
        .add_field(FieldBuilder::string("name".into()))
        .add_field(FieldBuilder::numeric("age".into()))
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
        .query("(name:Some AND age:19) OR age:21")
        .query_json(json!({
            "or": [
                {
                    "and": [
                        { "field": "name", "value": "Some" },
                        { "field": "age", "value": 19 }
                    ]
                },
                { "field": "age", "value": 21 }
            ]
        })).unwrap()
        .sort("name asc".into())
        .fl("name,age".into())
        .commit().await?;
    println!("{:#?}", users_found);

    Ok(())
}
