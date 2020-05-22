pub use tokio;
pub use serde;
pub use serde_json;

use std::fmt;
use serde_json::Value;
use serde_json::json;
use std::vec::Vec;
// use serde::{Serialize, Deserialize};

const MAX_CHAR_VAL: u32 = std::char::MAX as u32;

#[derive(Debug)]
pub struct SolrError;

impl std::error::Error for SolrError {}

impl fmt::Display for SolrError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "An Error Occurred, Please Try Again!")
    }
}

impl From<serde_json::Error> for SolrError {
    fn from(_error: serde_json::Error) -> Self {
        SolrError
    }
}

impl From<reqwest::Error> for SolrError {
    fn from(_error: reqwest::Error) -> Self {
        SolrError
    }
}

#[derive(Debug)]
pub struct Solr {
    pub protocol: String,
    pub host: String,
    pub port: u16,
}

impl Solr {
    pub fn client(protocol: String, host: String, port: u16) -> Solr {
        Solr { protocol, host, port }
    }

    /// # Source
    /// https://rosettacode.org/wiki/URL_encoding#Rust
    fn url_encode(&self, s: &String) -> String {
        let mut buff = [0; 4];

        s.chars()
            .map(|ch| {
                match ch as u32 {
                    0 ..= 47 | 58 ..= 64 | 91 ..= 96 | 123 ..= MAX_CHAR_VAL => {
                        ch.encode_utf8(&mut buff);
                        buff[0..ch.len_utf8()].iter().map(|&byte| format!("%{:X}", byte)).collect::<String>()
                    },
                    _ => ch.to_string(),
                }
            })
            .collect::<String>()
    }

    fn format_url(&self, s: &String) -> String {
        format!("{}://{}:{}/solr/{}", self.protocol, self.host, self.port, s)
    }

    async fn fetch(&self, path: &String) -> Result<serde_json::Value, SolrError> {
        let url = self.format_url(path);
        println!("Fetching: {}", url);
        let res = reqwest::get(&url).await?;
        let text: String = res.text().await?;
        let json: Value = match serde_json::from_str(&text) {
            Ok(r) => r,
            Err(_) => return Err(SolrError),
        };
        let err = json.get("error");
        if err.is_some() {
            let err = err.unwrap();
            println!("{:?}", err);
            return Err(SolrError);
        }
        Ok(json)
    }

    pub async fn get_system_info(&self) -> Result<serde_json::Value, SolrError> {
        let path = String::from("admin/info/system?wt=json");
        match self.fetch(&path).await {
            Ok(r) => Ok(r),
            Err(_) => return Err(SolrError),
        }
    }

    pub fn create_collection(&self, name: String) -> CollectionBuilder<'_> {
        let mut builder = CollectionBuilder::new(&self);
        builder.set_name(name);
        builder
    }

    pub async fn list_collections(&self) -> Result<Vec<Collection<'_>>, SolrError> {
        let path = String::from("admin/collections?action=LIST");
        let res = match self.fetch(&path).await {
            Ok(r) => r,
            Err(_) => return Err(SolrError),
        };

        let obj = match res["collections"].as_array().cloned() {
            Some(o) => o,
            None => return Err(SolrError),
        };

        let mut collections: Vec<Collection> = vec![];
        for c in obj.into_iter() {
            let name = String::from(c.as_str().unwrap());
            let col = Collection::new(self, name);
            collections.push(col);
        }
        Ok(collections)
    }

    pub async fn get_collection(&self, name: &String) -> Result<Collection<'_>, SolrError> {
        let path = String::from(format!("admin/collections?action=LIST"));
        let res = match self.fetch(&path).await {
            Ok(r) => r,
            Err(_) => return Err(SolrError),
        };
        for c in res["collections"].as_array().unwrap() {
            if c.as_str().unwrap().cmp(name.as_str()) == std::cmp::Ordering::Equal {
                return Ok(Collection::new(self, name.clone()));
            }
        }
        Err(SolrError)
    }

    pub async fn delete_collection(&self, name: &String) -> Result<(), SolrError> {
        let path = String::from(format!("admin/collections?action=DELETE&name={}", name));
        match self.fetch(&path).await {
            Ok(_) => Ok(()),
            Err(_) => Err(SolrError)
        }
    }
}

#[derive(Debug)]
pub struct Collection<'a> {
    client: &'a Solr,
    pub name: String,
    docs_to_commit: Vec<serde_json::Value>,
    error: Option<SolrError>,
}

impl<'a> Collection<'a> {
    fn new(client: &'a Solr, name: String) -> Collection<'a> {
        Collection {
            client: &client,
            name: name,
            docs_to_commit: vec![],
            error: None,
        }
    }

    pub fn search(&self) -> Query<'a, '_> {
        Query::new(&self)
    }

    pub fn add(&mut self, document: serde_json::Value) -> &mut Self {
        if document.is_array() {
            for doc in document.as_array().unwrap().clone() {
                if !doc.is_object() {
                    self.error = Some(SolrError);
                    break;
                }
                self.docs_to_commit.push(doc);
            }
        } else if document.is_object() {
            self.docs_to_commit.push(document);
        }
        self
    }

    pub fn get_commit_size(&self) -> usize {
        self.docs_to_commit.len()
    }

    pub async fn commit(&mut self) -> Result<(), SolrError> {
        if self.error.is_some() {
            let error = std::mem::replace(&mut self.error, None).unwrap();
            return Err(error);
        }

        if self.docs_to_commit.len() == 0 {
            println!("Info: No documents to commit, skipping...");
            return Ok(());
        }

        let path = format!("{}/update?commit=true", self.name);
        let res = match reqwest::Client::new().post(&self.client.format_url(&path))
            .json(&self.docs_to_commit)
            .send().await {
            Ok(_) => Ok(()),
            Err(_) => Err(SolrError),
        };
        self.docs_to_commit.clear();
        res
    }
}

#[derive(Debug)]
pub struct Field;

impl Field {
    pub fn text(name: String) -> serde_json::Value {
        json!({ "name": name, "options": { "type": "lowercase", "stored": true }})
    }

    pub fn string(name: String) -> serde_json::Value {
        json!({ "name": name, "type": "string", "stored": true, "omitNorms": true })
    }

    pub fn multi_string(name: String) -> serde_json::Value {
        json!({ "name": name, "type": "strings", "stored": true, "omitNorms": true, "multiValued": true })
    }

    pub fn numeric(name: String) -> serde_json::Value {
        json!({ "name": name, "type": "pfloat", "stored": true, "multiValued": false })
    }

    pub fn double(name: String) -> serde_json::Value {
        json!({ "name": name, "type": "pdouble", "stored": true })
    }

    pub fn long(name: String) -> serde_json::Value {
        json!({ "name": name, "type": "plong", "stored": true })
    }

    pub fn fulltext(name: String) -> serde_json::Value {
        json!({ "name": name, "type": "text_general", "stored": true })
    }

    pub fn tag(name: String) -> serde_json::Value {
        json!({ "name": name, "type": "delimited_payloads_string", "stored": true })
    }

    pub fn date(name: String) -> serde_json::Value {
        json!({ "name": name, "type": "pdate", "stored": true })
    }
}

#[derive(Debug)]
pub struct CollectionBuilder<'a> {
    client: &'a Solr,
    name: String,
    shard_count: u32,
    fields: Vec<serde_json::Value>,
}

impl<'a> CollectionBuilder<'a> {
    fn new<'b: 'a>(client: &'b Solr) -> CollectionBuilder<'a> {
        CollectionBuilder {
            client: &client,
            name: "".into(),
            shard_count: 1u32,
            fields: vec![],
        }
    }

    pub fn set_name(&mut self, name: String) -> &mut Self {
        self.name = name;
        self
    }

    pub fn set_shard_count(&mut self, shards: u32) -> &mut Self {
        self.shard_count = shards;
        self
    }

    pub fn add_field(&mut self, field: serde_json::Value) -> &mut Self {
        self.fields.push(field);
        self
    }

    pub async fn commit(&mut self) -> Result<Collection<'a>, SolrError> {
        let path = format!(
            "admin/collections?action=CREATE&name={}&numShards={}&maxShardsPerNode={}&router.field={}",
            self.name,
            self.shard_count,
            self.shard_count,
            "id");

        let res = match self.client.fetch(&path).await {
            Ok(r) => r,
            Err(_) => return Err(SolrError),
        };

        if res.get("success").is_none() {
            return Err(SolrError);
        }

        let col = Collection::new(&self.client, self.name.clone());

        let path = format!("{}/schema", self.name);

        // TODO: Check if scheme was created!
        let _res = match reqwest::Client::new().post(&self.client.format_url(&path))
            .json(&json!({
                "add-field": &self.fields
            }))
            .send().await {
            Ok(_) => Ok(()),
            Err(_) => Err(SolrError),
        };

        Ok(col)
    }
}

#[derive(Debug)]
pub struct Query<'a, 'b> {
    collection: &'a Collection<'b>,
    query_str: Option<String>,
    fields_str: Option<String>,
}

impl<'a, 'b> Query<'a, 'b> {
    fn new(collection: &'b Collection) -> Query<'a, 'b> {
        Query {
            collection: &collection,
            query_str: None,
            fields_str: None,
        }
    }

    pub fn query(&mut self, query: String) -> &mut Self {
        self.query_str = Some(query);
        self
    }

    pub fn fields(&mut self, fields: String) -> &mut Self {
        self.fields_str = Some(fields);
        self
    }

    pub async fn commit(&self) -> Result<Vec<serde_json::Value>, SolrError> {
        if self.query_str.is_none() {
            return Err(SolrError);
        }
        let q = self.query_str.as_ref().unwrap();
        let q = self.collection.client.url_encode(q);
        let path = format!("{}/select?q={}", self.collection.name, q);
        let res = match self.collection.client.fetch(&path).await {
            Ok(r) => r,
            Err(_) => return Err(SolrError),
        };
        Ok(res["response"]["docs"].as_array().unwrap().clone())
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }
}
