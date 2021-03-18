# Solrdrv
> Solr driver for the Rust language

# Table of Contents
* [Example](#example)
* [Solr for Testing Purposes](#solr-for-testing-purposes)

# Example
```rust
extern crate solrdrv;

use solrdrv::{
    serde_json::json,
    tokio,
    Solr,
    FieldBuilder
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Create a client for our local database
    let solr = Solr::client("http".into(), "localhost".into(), 8983);

    // Create a new collection `users`
    let mut users = solr.collections()
        .create("users".into())
        .router_field("id".into())
        .num_shards(16)
        .max_shards_per_node(16)
        .commit().await?;

    // Add fields `name` and `age` into its schema (using our prebuilt types)
    users.schema()
        .add_field(FieldBuilder::string("name".into()))
        .add_field(FieldBuilder::numeric("age".into()))
        .commit().await?;

    // Add documents
    users.add(json!([
        {
            "name": "Some",
            "age": 19
        },
        {
            "name": "Dude",
            "age": 21
        }
    ])).commit().await?;

    // Query and print added documents
    let users_found = users.search()
        .query("(name:Some AND age:19) OR age:21".into())
        .sort("name asc".into())
        .fl("name,age".into())
        .commit().await?;
    println!("{:#?}", users_found);

    Ok(())
}
```

# Solr for Testing Purposes
If you need an instance of Solr for testing purposes, you can run one using Docker:
```sh
docker run -p 8983:8983 -t solr -c
```
