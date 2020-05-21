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
    protocol: String,
    host: String,
    port: u16,
}

impl SolrDrv {
    pub fn new(protocol: String, host: String, port: u16) -> SolrDrv {
        SolrDrv {
            protocol,
            host,
            port,
        }
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
        if json.get("error").is_some() {
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

    pub async fn create_collection(&self, _name: String) {
    }

    pub async fn list_collections(&self) -> Result<Vec<SolrCollection<'_>>, SolrError> {
        let path = String::from("admin/collections?action=CLUSTERSTATUS");
        let res = match self.fetch(&path).await {
            Ok(r) => r,
            Err(_) => return Err(SolrError),
        };

        let obj = res["cluster"]["collections"].as_object().cloned();
        if obj.is_none() {
            return Err(SolrError);
        }
        let obj = obj.unwrap();

        let mut collections: Vec<SolrCollection> = vec![];
        for c in obj.into_iter() {
            let col = SolrCollection::new(self, String::from(c.0), c.1);
            collections.push(col);
        }
        Ok(collections)
    }

    pub async fn get_collection(&self, name: &String) -> Result<SolrCollection<'_>, SolrError> {
        let path = String::from(format!("admin/collections?action=CLUSTERSTATUS&collection={}", name));
        let res = match self.fetch(&path).await {
            Ok(r) => r,
            Err(_) => return Err(SolrError),
        };
        let collection = res["cluster"]["collections"][&name].clone();
        Ok(SolrCollection::new(self, name.clone(), collection))
    }
}

#[derive(Debug)]
pub struct SolrCollection<'a> {
    driver: &'a SolrDrv,
    name: String,
    serialized: Value,
}

impl SolrCollection<'_> {
    pub fn new(driver: &SolrDrv, name: String, serialized: Value) -> SolrCollection {
        SolrCollection {
            driver,
            name,
            serialized,
        }
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
