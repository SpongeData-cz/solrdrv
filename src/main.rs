extern crate solrdrv;

use solrdrv::{
    serde_json::json,
    tokio,
    Solr,
    Field
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let solr = Solr::client("http".into(), "localhost".into(), 8983);

    let _res = match solr.get_collection(&"users".into()).await {
        Ok(col) => solr.delete_collection(&col.name).await,
        Err(_) => Ok(())
    };

    let mut users = solr.create_collection("users".into())
        .set_shard_count(16)
        .add_field(Field::string("name".into()))
        .add_field(Field::numeric("age".into()))
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
        // .from_json(json!(...))
        .fields("name".into())
        .commit().await?;
    println!("{:#?}", users_found);

    Ok(())
}
