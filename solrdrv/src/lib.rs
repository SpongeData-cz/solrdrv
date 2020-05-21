pub use tokio;
pub use serde;
pub use serde_json;

use std::fmt;
use serde_json::Value;
use std::vec::Vec;
use std::error::Error;
// use serde::{Serialize, Deserialize};

pub struct SolrError;

impl Error for SolrError {}

impl fmt::Display for SolrError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "An Error Occurred, Please Try Again!")
    }
}

impl fmt::Debug for SolrError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{{ file: {}, line: {} }}", file!(), line!())
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
pub struct SolrDrv {
    pub protocol: String,
    pub host: String,
    pub port: u16,
}

impl SolrDrv {
    pub fn new(protocol: String, host: String, port: u16) -> SolrDrv {
        SolrDrv { protocol, host, port }
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

    pub async fn create_collection(&self, name: &String) -> Result<SolrCollection<'_>, SolrError> {
        // TODO: Replace hardcoded shard count with configuration API
        let path = String::from(format!(
            "admin/collections?action=CREATE&name={}&numShards={}&maxShardsPerNode={}&router.field={}",
            name, 16, 16, "id"));
        let res = match self.fetch(&path).await {
            Ok(r) => r,
            Err(_) => return Err(SolrError),
        };

        if res.get("success").is_none() {
            return Err(SolrError);
        }

        Ok(SolrCollection::new(&self, name.clone()))
    }

    pub async fn list_collections(&self) -> Result<Vec<SolrCollection<'_>>, SolrError> {
        let path = String::from("admin/collections?action=LIST");
        let res = match self.fetch(&path).await {
            Ok(r) => r,
            Err(_) => return Err(SolrError),
        };

        let obj = match res["collections"].as_array().cloned() {
            Some(o) => o,
            None => return Err(SolrError),
        };

        let mut collections: Vec<SolrCollection> = vec![];
        for c in obj.into_iter() {
            let name = String::from(c.as_str().unwrap());
            let col = SolrCollection::new(self, name);
            collections.push(col);
        }
        Ok(collections)
    }

    pub async fn get_collection(&self, name: &String) -> Result<SolrCollection<'_>, SolrError> {
        let path = String::from(format!("admin/collections?action=LIST&collection={}", name));
        match self.fetch(&path).await {
            Ok(_) => Ok(SolrCollection::new(self, name.clone())),
            Err(_) => Err(SolrError),
        }
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
pub struct SolrCollection<'a> {
    pub driver: &'a SolrDrv,
    pub name: String
}

impl SolrCollection<'_> {
    pub fn new(driver: &SolrDrv, name: String) -> SolrCollection {
        SolrCollection { driver, name }
    }

    pub async fn select(&self, query: &String) -> Result<Vec<Value>, SolrError> {
        let path = String::from(format!("{}/select?q={}", self.name, query));
        let res = match self.driver.fetch(&path).await {
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
