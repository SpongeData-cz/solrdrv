//! Solrdrv is an unofficial Solr driver for the Rust programming language.

pub use tokio;
pub use serde;
pub use serde_json;

use std::fmt;
use std::vec::Vec;
use serde_json::json;
use serde_json::Value;
// use serde::{Serialize, Deserialize};

const MAX_CHAR_VAL: u32 = std::char::MAX as u32;

#[derive(Debug)]
/// A common error type used by this library
pub struct SolrError;

impl std::error::Error for SolrError {}

impl fmt::Display for SolrError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "An error occurred!")
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
/// A Solr client
pub struct Solr {
    /// A protocol on which is the Solr API available (e.g. `http`, `https`).
    pub protocol: String,
    /// A host name on which is the Solr API available (e.g. `localhost`).
    pub host: String,
    /// A port on which is the Solr API available (e.g. `8983`).
    pub port: u16,
}

impl Solr {
    /// Creates a new client for a Solr database.
    ///
    /// # Arguments
    /// * `protocol` -
    /// * `host` -
    /// * `port` -
    ///
    /// # Example
    /// ```
    /// let client = Solr.client("http".into(), "localhost".into(), 8983);
    /// ```
    pub fn client(protocol: String, host: String, port: u16) -> Solr {
        Solr { protocol, host, port }
    }

    /// Percentage-encodes unsafe characters of a URL parameter value.
    ///
    /// # Arguments
    /// * `string` - The string to encode.
    ///
    /// # Example
    /// ```
    /// client.url_encode(&"date: [2020-05-26 TO *]".into());
    /// // => date%3A%20%5B2020-05-26%20TO%20%2A%5D
    /// ```
    ///
    /// # Source
    /// https://rosettacode.org/wiki/URL_encoding#Rust
    pub fn url_encode(&self, string: &String) -> String {
        let mut buff = [0; 4];

        string.chars()
            .map(|ch| {
                match ch as u32 {
                    0..=47 | 58..=64 | 91..=96 | 123..=MAX_CHAR_VAL => {
                        ch.encode_utf8(&mut buff);
                        buff[0..ch.len_utf8()].iter().map(|&byte| format!("%{:X}", byte)).collect::<String>()
                    }
                    _ => ch.to_string(),
                }
            })
            .collect::<String>()
    }

    /// Creates a string using format "{protocol}://{host}:{port}/{path}".
    ///
    /// # Arguments
    /// * `path` -
    pub fn format_url(&self, path: &String) -> String {
        format!("{}://{}:{}/solr/{}", self.protocol, self.host, self.port, path)
    }

    async fn parse_fetch_result(&self, res: reqwest::Response) -> Result<serde_json::Value, SolrError> {
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

    /// Fetches a result of a GET request for the specified path.
    ///
    /// # Arguments
    /// * `path` -
    ///
    /// # Example
    /// ```
    /// let res = match client.get(&"admin/collections?action=LIST".into()).await {
    ///     Ok(r) => r,
    ///     Err(e) => return e,
    /// };
    ///
    /// ```
    ///
    /// # Return
    /// If the fetch fails or the result contains an "error" key, then returns a `SolrError`,
    /// otherwise returns the fetched result.
    pub async fn get(&self, path: &String) -> Result<serde_json::Value, SolrError> {
        let url = self.format_url(path);
        println!("GET: {}", url);
        let res = reqwest::get(&url).await?;
        self.parse_fetch_result(res).await
    }

    /// Fetches a result of a POST request for the specified path.
    ///
    /// # Arguments
    /// * `path` -
    /// * `data` -
    ///
    /// # Example
    /// ```
    /// let data = json!({ "add-field": {
    ///     "name": "birthday",
    ///     "type": "pdate",
    ///     "stored": true } });
    /// let res = match client.post(&"users/schema".into(), &data).await {
    ///     Ok(r) => r,
    ///     Err(e) => return e,
    /// };
    /// ```
    ///
    /// # Return
    /// If the fetch fails or the result contains an "error" key, then returns a `SolrError`,
    /// otherwise returns the fetched result.
    pub async fn post(&self, path: &String, data: &serde_json::Value) -> Result<serde_json::Value, SolrError> {
        let url = self.format_url(path);
        println!("POST: {}", url);
        let client = reqwest::Client::new();
        let res = client.post(&self.format_url(&path)).json(&data).send().await?;
        self.parse_fetch_result(res).await
    }

    pub async fn get_system_info(&self) -> Result<serde_json::Value, SolrError> {
        let path = String::from("admin/info/system?wt=json");
        match self.get(&path).await {
            Ok(r) => Ok(r),
            Err(_) => Err(SolrError),
        }
    }

    /// Returns a `CollectionAPI` struct, which can be used to create and manage collections.
    pub fn collections(&self) -> CollectionsAPI {
        CollectionsAPI::new(&self)
    }
}

#[derive(Debug)]
/// An API for managing collections
pub struct CollectionsAPI<'a> {
    client: &'a Solr
}

impl<'a> CollectionsAPI<'a> {
    fn new(client: &'a Solr) -> CollectionsAPI<'a> {
        CollectionsAPI {
            client: &client
        }
    }

    /// Returns a `CollectionBuilder` structure using which you can define and create new
    /// collections.
    ///
    /// # Arguments
    /// * `name` - The name of the collection.
    pub fn create(&self, name: String) -> CollectionBuilder<'a> {
        CollectionBuilder::new(&self.client, name);
    }

    /// Returns a list of existing collections.
    pub async fn list(&self) -> Result<Vec<Collection<'_>>, SolrError> {
        let path = String::from("admin/collections?action=LIST");
        let res = match self.client.get(&path).await {
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
            let col = Collection::new(&self.client, name);
            collections.push(col);
        }
        Ok(collections)
    }

    /// Returns an already existing collection with specified name.
    ///
    /// # Arguments
    /// * `name` - The name of the collection to retrieve.
    pub async fn get(&self, name: String) -> Result<Collection<'_>, SolrError> {
        let path = String::from(format!("admin/collections?action=LIST"));
        let res = match self.client.get(&path).await {
            Ok(r) => r,
            Err(_) => return Err(SolrError),
        };
        for c in res["collections"].as_array().unwrap() {
            if c.as_str().unwrap().cmp(name.as_str()) == std::cmp::Ordering::Equal {
                return Ok(Collection::new(&self.client, name.clone()));
            }
        }
        Err(SolrError)
    }

    /// Deletes an existing collection with specified name.
    ///
    /// # Arguments
    /// * `name` - The name of the collection to delete.
    pub async fn delete(&self, name: &String) -> Result<(), SolrError> {
        let path = String::from(format!("admin/collections?action=DELETE&name={}", name));
        match self.client.get(&path).await {
            Ok(_) => Ok(()),
            Err(_) => Err(SolrError)
        }
    }
}

#[derive(Debug)]
/// An abstraction of a single existing collection
pub struct Collection<'a> {
    client: &'a Solr,
    /// The name of the collection.
    pub name: String,
    /// Docs enqueued for commit.
    docs_to_commit: Vec<serde_json::Value>,
    /// Set if an error occurs during docs commit.
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

    /// Returns a `SchemaAPI` struct which is used to modify schema of a collection.
    pub fn schema(&self) -> SchemaAPI<'a, '_> {
        SchemaAPI::new(&self)
    }

    /// Returns a `Query` struct which is used to search for documents within a collection.
    pub fn search(&self) -> Query<'a, '_> {
        Query::new(&self)
    }

    /// Enqueues a document to be added into a collection. Use `commit` to actually send the enqueued
    /// documents.
    ///
    /// # Arguments
    /// * `document` - Can be either an object for single document or an array of objects for
    /// multiple documents.
    ///
    /// # Example
    /// ```
    /// users.add(json!({ "name": "Some", "age": 19 }))
    ///     .add(json!({ "name": "Dude", "age": 21 }));
    ///
    /// // ^ is the same as:
    ///
    /// users.add(json!([
    ///     { "name": "Some", "age": 19 },
    ///     { "name": "Dude", "age": 21 }
    /// ]));
    /// ```
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

    /// Return a number of documents enqueued for adding into a collection.
    pub fn get_commit_size(&self) -> usize {
        self.docs_to_commit.len()
    }

    /// Sends enqueued documents into a collection.
    ///
    /// # Example
    /// ```
    /// users.add(json!({"name": "Some" })).commit().await?;
    /// ```
    pub async fn commit(&mut self) -> Result<(), SolrError> {
        if self.error.is_some() {
            let error = std::mem::replace(&mut self.error, None).unwrap();
            return Err(error);
        }

        if self.docs_to_commit.is_empty() {
            println!("Info: No documents to commit, skipping...");
            return Ok(());
        }

        let path = format!("{}/update?commit=true", self.name);
        let res = match self.client.post(&path, &json!(self.docs_to_commit)).await {
            Ok(_) => Ok(()),
            Err(_) => Err(SolrError),
        };
        self.docs_to_commit.clear();
        res
    }
}

#[derive(Debug)]
/// A builder for collections
pub struct CollectionBuilder<'a> {
    client: &'a Solr,
    // Source: https://lucene.apache.org/solr/guide/8_5/collection-management.html#create
    name: String,
    num_shards: Option<usize>,
    max_shards_per_node: Option<usize>,
    router_field: Option<String>,
}

impl<'a> CollectionBuilder<'a> {
    fn new<'b: 'a>(client: &'b Solr, name: String) -> CollectionBuilder<'a> {
        CollectionBuilder {
            client: &client,
            name: name,
            num_shards: None,
            max_shards_per_node: None,
            router_field: None,
        }
    }

    /// Set the number of shards to be created as part of the collection.
    ///
    /// # Arguments
    /// * `num_shards` -
    ///
    /// # See
    /// https://lucene.apache.org/solr/guide/8_5/collection-management.html#create
    pub fn num_shards(&mut self, num_shards: usize) -> &mut Self {
        self.num_shards = Some(num_shards);
        self
    }

    /// Set the maximum number of shards per node.
    ///
    /// # Arguments
    /// * `max_shards_per_node` -
    ///
    /// # See
    /// https://lucene.apache.org/solr/guide/8_5/collection-management.html#create
    pub fn max_shards_per_node(&mut self, max_shards_per_node: usize) -> &mut Self {
        self.max_shards_per_node = Some(max_shards_per_node);
        self
    }

    /// Set the name of the field used to compute a hash.
    ///
    /// # Arguments
    /// * `router_field`-
    ///
    /// # See
    /// https://lucene.apache.org/solr/guide/8_5/collection-management.html#create
    pub fn router_field(&mut self, router_field: String) -> &mut Self {
        self.router_field = Some(router_field);
        self
    }

    fn build_path(&self) -> String {
        let mut path = format!("admin/collections?action=CREATE&name={}", self.name);

        if self.num_shards.is_some() {
            let temp = self.num_shards.as_ref().unwrap();
            path = format!("{}&numShards={}", path, temp);
        }

        if self.max_shards_per_node.is_some() {
            let temp = self.max_shards_per_node.as_ref().unwrap();
            path = format!("{}&maxShardsPerNode={}", path, temp);
        }

        if self.router_field.is_some() {
            let temp = self.router_field.as_ref().unwrap();
            let temp = self.client.url_encode(&temp);
            path = format!("{}&router.field={}", path, temp);
        }

        path
    }

    /// Creates a new collection with specified properties.
    ///
    /// # Example
    /// Following example creates a new `users` collection.
    /// ```
    /// let mut users = solr.collections()
    ///     .create("users".into())
    ///     .router_field("id".into())
    ///     .num_shards(16)
    ///     .max_shards_per_node(16)
    ///     .commit().await?;
    /// ```
    pub async fn commit(&mut self) -> Result<Collection<'a>, SolrError> {
        if self.name.is_empty() {
            return Err(SolrError);
        }
        let path = self.build_path();
        let res = match self.client.get(&path).await {
            Ok(r) => r,
            Err(_) => return Err(SolrError),
        };
        if res.get("success").is_none() {
            return Err(SolrError);
        }
        let col = Collection::new(&self.client, self.name.clone());
        Ok(col)
    }
}

#[derive(Debug)]
/// A builder for schema fields
pub struct FieldBuilder {
    // Source: https://lucene.apache.org/solr/guide/8_5/defining-fields.html#field-properties
    name: String,
    typename: String, // Should be just `type`, but that is a registered keyword in Rust
    default: Option<serde_json::Value>,
    // Source: https://lucene.apache.org/solr/guide/8_5/defining-fields.html#optional-field-type-override-properties
    indexed: Option<bool>,
    stored: Option<bool>,
    doc_values: Option<bool>,
    sort_missing_first: Option<bool>,
    sort_missing_last: Option<bool>,
    multi_valued: Option<bool>,
    uninvertible: Option<bool>,
    omit_norms: Option<bool>,
    omit_term_freq_and_positions: Option<bool>,
    omit_positions: Option<bool>,
    term_vectors: Option<bool>,
    term_positions: Option<bool>,
    term_offsets: Option<bool>,
    term_payloads: Option<bool>,
    required: Option<bool>,
    use_doc_values_as_stored: Option<bool>,
    large: Option<bool>
}

impl FieldBuilder {
    /// Creates a new field builder.
    ///
    /// # Arguments
    /// * `name` - The name of the field.
    pub fn new(name: String) -> FieldBuilder {
        FieldBuilder {
            name: name,
            typename: "".into(),
            default: None,
            indexed: None,
            stored: None,
            doc_values: None,
            sort_missing_first: None,
            sort_missing_last: None,
            multi_valued: None,
            uninvertible: None,
            omit_norms: None,
            omit_term_freq_and_positions: None,
            omit_positions: None,
            term_vectors: None,
            term_positions: None,
            term_offsets: None,
            term_payloads: None,
            required: None,
            use_doc_values_as_stored: None,
            large: None
        }
    }

    /// Sets the type of the field.
    ///
    /// # Arguments
    /// * `typename` -
    ///
    /// # See
    /// https://lucene.apache.org/solr/guide/8_5/defining-fields.html#field-properties
    pub fn typename(&mut self, typename: String) -> &mut Self {
        self.typename = typename;
        self
    }

    /// Sets a default value for documents without the field.
    ///
    /// # Arguments
    /// * `default` -
    ///
    /// # See
    /// https://lucene.apache.org/solr/guide/8_5/defining-fields.html#field-properties
    pub fn default(&mut self, default: serde_json::Value) -> &mut Self {
        self.default = Some(default);
        self
    }

    /// Sets whether the field can be used in queries.
    ///
    /// # Arguments
    /// * `indexed` -
    ///
    /// # See
    /// https://lucene.apache.org/solr/guide/8_5/defining-fields.html#optional-field-type-override-properties
    pub fn indexed(&mut self, indexed: bool) -> &mut Self {
        self.indexed = Some(indexed);
        self
    }

    /// Sets whether the field's value can be retrieved with queries.
    ///
    /// # Arguments
    /// * `stored` -
    ///
    /// # See
    /// https://lucene.apache.org/solr/guide/8_5/defining-fields.html#optional-field-type-override-properties
    pub fn stored(&mut self, stored: bool) -> &mut Self {
        self.stored = Some(stored);
        self
    }

    /// Sets whether the field's value should be put into a DocValues structure.
    ///
    /// # Arguments
    /// * `doc_values` -
    ///
    /// # See
    /// https://lucene.apache.org/solr/guide/8_5/defining-fields.html#optional-field-type-override-properties
    pub fn doc_values(&mut self, doc_values: bool) -> &mut Self {
        self.doc_values = Some(doc_values);
        self
    }

    /// Control the placement of documents when a sort field is not present.
    ///
    /// # Arguments
    /// * `sort_missing_first` -
    ///
    /// # See
    /// https://lucene.apache.org/solr/guide/8_5/defining-fields.html#optional-field-type-override-properties
    pub fn sort_missing_first(&mut self, sort_missing_first: bool) -> &mut Self {
        self.sort_missing_first = Some(sort_missing_first);
        self
    }

    /// Control the placement of documents when a sort field is not present.
    ///
    /// # Arguments
    /// * `sort_missing_last` -
    ///
    /// # See
    /// https://lucene.apache.org/solr/guide/8_5/defining-fields.html#optional-field-type-override-properties
    pub fn sort_missing_last(&mut self, sort_missing_last: bool) -> &mut Self {
        self.sort_missing_last = Some(sort_missing_last);
        self
    }

    /// Sets whether the field can contain multiple values of its type.
    ///
    /// # Arguments
    /// * `multi_valued` -
    ///
    /// # See
    /// https://lucene.apache.org/solr/guide/8_5/defining-fields.html#optional-field-type-override-properties
    pub fn multi_valued(&mut self, multi_valued: bool) -> &mut Self {
        self.multi_valued = Some(multi_valued);
        self
    }

    /// Sets whether the field can be "un-inverted" at query time.
    ///
    /// # Arguments
    /// * `uninvertible` -
    ///
    /// # See
    /// https://lucene.apache.org/solr/guide/8_5/defining-fields.html#optional-field-type-override-properties
    pub fn uninvertible(&mut self, uninvertible: bool) -> &mut Self {
        self.uninvertible = Some(uninvertible);
        self
    }

    /// Sets whether norms associated with this field should be omitted.
    ///
    /// # Arguments
    /// * `omit_norms` -
    ///
    /// # See
    /// https://lucene.apache.org/solr/guide/8_5/defining-fields.html#optional-field-type-override-properties
    pub fn omit_norms(&mut self, omit_norms: bool) -> &mut Self {
        self.omit_norms = Some(omit_norms);
        self
    }

    /// Sets whether term frequency, positions, and payloads from postings for this field should be
    /// omitted.
    ///
    /// # Arguments
    /// * `omit_term_freq_and_positions` -
    ///
    /// # See
    /// https://lucene.apache.org/solr/guide/8_5/defining-fields.html#optional-field-type-override-properties
    pub fn omit_term_freq_and_positions(&mut self, omit_term_freq_and_positions: bool) -> &mut Self {
        self.omit_term_freq_and_positions = Some(omit_term_freq_and_positions);
        self
    }

    /// Similar to `omit_term_freq_and_positions`, but preserves term frequency information.
    ///
    /// # Arguments
    /// * `omit_positions` -
    ///
    /// # See
    /// https://lucene.apache.org/solr/guide/8_5/defining-fields.html#optional-field-type-override-properties
    pub fn omit_positions(&mut self, omit_positions: bool) -> &mut Self {
        self.omit_positions = Some(omit_positions);
        self
    }

    /// Enables maintaining term vectors.
    ///
    /// # Arguments
    /// * `term_vectors` -
    ///
    /// # See
    /// https://lucene.apache.org/solr/guide/8_5/defining-fields.html#optional-field-type-override-properties
    pub fn term_vectors(&mut self, term_vectors: bool) -> &mut Self {
        self.term_vectors = Some(term_vectors);
        self
    }

    /// Enables maintaining position information for each term occurrence.
    ///
    /// # Arguments
    /// * `term_positions` -
    ///
    /// # See
    /// https://lucene.apache.org/solr/guide/8_5/defining-fields.html#optional-field-type-override-properties
    pub fn term_positions(&mut self, term_positions: bool) -> &mut Self {
        self.term_positions = Some(term_positions);
        self
    }

    /// Enables maintaining offset information for each term occurrence.
    ///
    /// # Arguments
    /// * `term_offsets` -
    ///
    /// # See
    /// https://lucene.apache.org/solr/guide/8_5/defining-fields.html#optional-field-type-override-properties
    pub fn term_offsets(&mut self, term_offsets: bool) -> &mut Self {
        self.term_offsets = Some(term_offsets);
        self
    }

    /// Enables maintaining payload information for each term occurrence.
    ///
    /// # Arguments
    /// * `term_payloads` -
    ///
    /// # See
    /// https://lucene.apache.org/solr/guide/8_5/defining-fields.html#optional-field-type-override-properties
    pub fn term_payloads(&mut self, term_payloads: bool) -> &mut Self {
        self.term_payloads = Some(term_payloads);
        self
    }

    /// Sets whether documents without this field should be rejected.
    ///
    /// # Arguments
    /// * `required` -
    ///
    /// # See
    /// https://lucene.apache.org/solr/guide/8_5/defining-fields.html#optional-field-type-override-properties
    pub fn required(&mut self, required: bool) -> &mut Self {
        self.required = Some(required);
        self
    }

    /// Enables returning `doc_value`s as if they were stored.
    ///
    /// # Arguments
    /// * `use_doc_values_as_stored` -
    ///
    /// # See
    /// https://lucene.apache.org/solr/guide/8_5/defining-fields.html#optional-field-type-override-properties
    pub fn use_doc_values_as_stored(&mut self, use_doc_values_as_stored: bool) -> &mut Self {
        self.use_doc_values_as_stored = Some(use_doc_values_as_stored);
        self
    }

    /// Enables lazy load.
    ///
    /// # Arguments
    /// * `large` -
    ///
    /// # See
    /// https://lucene.apache.org/solr/guide/8_5/defining-fields.html#optional-field-type-override-properties
    pub fn large(&mut self, large: bool) -> &mut Self {
        self.large = Some(large);
        self
    }

    /// Returns a prebuilt `text` field.
    pub fn text(name: String) -> serde_json::Value {
        FieldBuilder::new(name)
            .typename("lowercase".into())
            .stored(true)
            .build().unwrap()
    }

    /// Returns a prebuilt `string` field.
    pub fn string(name: String) -> serde_json::Value {
        FieldBuilder::new(name)
            .typename("string".into())
            .omit_norms(true)
            .stored(true)
            .build().unwrap()
    }

    /// Returns a prebuilt `multi string` field.
    pub fn multi_string(name: String) -> serde_json::Value {
        FieldBuilder::new(name)
            .typename("strings".into())
            .omit_norms(true)
            .multi_valued(true)
            .stored(true)
            .build().unwrap()
    }

    /// Returns a prebuilt `numeric` field.
    pub fn numeric(name: String) -> serde_json::Value {
        FieldBuilder::new(name)
            .typename("pfloat".into())
            .stored(true)
            .build().unwrap()
    }

    /// Returns a prebuilt `double` field.
    pub fn double(name: String) -> serde_json::Value {
        FieldBuilder::new(name)
            .typename("pdouble".into())
            .stored(true)
            .build().unwrap()
    }

    /// Returns a prebuilt `long` field.
    pub fn long(name: String) -> serde_json::Value {
        FieldBuilder::new(name)
            .typename("plong".into())
            .stored(true)
            .build().unwrap()
    }

    /// Returns a prebuilt `fulltext` field.
    pub fn fulltext(name: String) -> serde_json::Value {
        FieldBuilder::new(name)
            .typename("text_general".into())
            .stored(true)
            .build().unwrap()
    }

    /// Returns a prebuilt `tag` field.
    pub fn tag(name: String) -> serde_json::Value {
        FieldBuilder::new(name)
            .typename("delimited_payloads_string".into())
            .stored(true)
            .build().unwrap()
    }

    /// Returns a prebuilt `date` field.
    pub fn date(name: String) -> serde_json::Value {
        FieldBuilder::new(name)
            .typename("pdate".into())
            .stored(true)
            .build().unwrap()
    }

    /// Builds a new field descriptor with specified properties.
    ///
    /// # Example
    /// ```
    /// let name = FieldBuilder::new("name".into())
    ///     .typename("string".into())
    ///     .omit_norms(true)
    ///     .stored(true)
    ///     .build().unwrap();
    /// ```
    pub fn build(&self) -> Result<serde_json::Value, SolrError> {
        if self.name.is_empty()
            || self.typename.is_empty() {
            return Err(SolrError);
        }

        let mut json = json!({
            "name": self.name,
            "type": self.typename
        });

        if self.default.is_some() {
            json["default"] = json!(self.default.as_ref().unwrap());
        }

        if self.indexed.is_some() {
            json["indexed"] = json!(self.indexed.unwrap());
        }

        if self.stored.is_some() {
            json["stored"] = json!(self.stored.unwrap());
        }

        if self.doc_values.is_some() {
            json["docValues"] = json!(self.doc_values.unwrap());
        }

        if self.sort_missing_first.is_some() {
            json["sortMissingFirst"] = json!(self.sort_missing_first.unwrap());
        }

        if self.sort_missing_last.is_some() {
            json["sortMissingLast"] = json!(self.sort_missing_last.unwrap());
        }

        if self.multi_valued.is_some() {
            json["multiValued"] = json!(self.multi_valued.unwrap());
        }

        if self.uninvertible.is_some() {
            json["uninvertible"] = json!(self.uninvertible.unwrap());
        }

        if self.omit_norms.is_some() {
            json["omitNorms"] = json!(self.omit_norms.unwrap());
        }

        if self.omit_term_freq_and_positions.is_some() {
            json["omitTermFreqAndPositions"] = json!(self.omit_term_freq_and_positions.unwrap());
        }

        if self.omit_positions.is_some() {
            json["omitPositions"] = json!(self.omit_positions.unwrap());
        }

        if self.term_vectors.is_some() {
            json["termVectors"] = json!(self.term_vectors.unwrap());
        }

        if self.term_positions.is_some() {
            json["termPositions"] = json!(self.term_positions.unwrap());
        }

        if self.term_offsets.is_some() {
            json["termOffsets"] = json!(self.term_offsets.unwrap());
        }

        if self.term_payloads.is_some() {
            json["termPayloads"] = json!(self.term_payloads.unwrap());
        }

        if self.required.is_some() {
            json["required"] = json!(self.required.unwrap());
        }

        if self.use_doc_values_as_stored.is_some() {
            json["useDocValuesAsStored"] = json!(self.use_doc_values_as_stored.unwrap());
        }

        if self.large.is_some() {
            json["large"] = json!(self.large.unwrap());
        }

        Ok(json)
    }
}

#[derive(Debug)]
/// A schema API
pub struct SchemaAPI<'a, 'b> {
    collection: &'a Collection<'b>,
    fields_to_add: Vec<serde_json::Value>,
    fields_to_delete: Vec<String>,
    fields_to_replace: Vec<serde_json::Value>,
}

impl<'a, 'b> SchemaAPI<'a, 'b> {
    fn new(collection: &'a Collection<'b>) -> SchemaAPI<'a, 'b> {
        SchemaAPI {
            collection: &collection,
            fields_to_add: vec![],
            fields_to_delete: vec![],
            fields_to_replace: vec![],
        }
    }

    /// Retrieves a schema of a collection.
    ///
    /// # See
    /// https://lucene.apache.org/solr/guide/8_5/schema-api.html#retrieve-schema-information
    pub async fn get(&self) -> Result<serde_json::Value, SolrError> {
        let path = format!("{}/schema", self.collection.name);
        self.collection.client.get(&path).await
    }

    /// Enqueues a command to add a new field to a collection. Use `commit` to actually execute all
    /// enqueued commands.
    ///
    /// # Arguments
    /// * `field` - The new field to be added.
    ///
    /// # Example
    /// Following example adds fields `name` and `age` into a collection `users` and commits the
    /// changes.
    /// ```
    /// users.schema()
    ///     .add_field(FieldBuilder::string("name".into()))
    ///     .add_field(FieldBuilder::numeric("age".into()))
    ///     .commit().await?;
    /// ```
    ///
    /// # See
    /// https://lucene.apache.org/solr/guide/8_5/schema-api.html
    pub fn add_field(&mut self, field: serde_json::Value) -> &mut Self {
        self.fields_to_add.push(field);
        self
    }

    /// Enqueues a command to delete an existing field from a collection scheme. Use `commit`
    /// to actually execute all enqueued commands.
    ///
    /// # Arguments
    /// * `name` - The name of the field to delete.
    ///
    /// # See
    /// https://lucene.apache.org/solr/guide/8_5/schema-api.html#delete-a-field
    pub fn delete_field(&mut self, name: String) -> &mut Self {
        self.fields_to_delete.push(name);
        self
    }

    /// Enqueues a command to replace a definition of an already existing field. Use `commit`
    /// to actually execute all enqueued commands.
    ///
    /// # Arguments
    /// * `field` - The new field definition.
    ///
    /// # See
    /// https://lucene.apache.org/solr/guide/8_5/schema-api.html#replace-a-field
    pub fn replace_field(&mut self, field: serde_json::Value) -> &mut Self {
        self.fields_to_replace.push(field);
        self
    }

    /// Commits all enqueued commands.
    ///
    /// # Example
    /// Following code adds a field `name` into a collection `users`, removes its field `age` and
    /// commits the changes.
    /// ```
    /// users.scheme()
    ///     .add_field(FieldBuilder::string("name".into()))
    ///     .delete_field("age".into())
    ///     .commit().await?;
    /// ```
    pub async fn commit(&mut self) -> Result<(), SolrError> {
        if self.fields_to_add.is_empty()
            && self.fields_to_delete.is_empty()
            && self.fields_to_replace.is_empty() {
            println!("Info: No schema changes to commit, skipping...");
            return Ok(());
        }

        let path = format!("{}/schema", self.collection.name);
        let mut data = json!({});

        if !self.fields_to_add.is_empty() {
            data["add-field"] = json!(self.fields_to_add);
            self.fields_to_add.clear();
        }

        if !self.fields_to_delete.is_empty() {
            data["delete-field"] = json!(self.fields_to_delete);
            self.fields_to_add.clear();
        }

        if !self.fields_to_replace.is_empty() {
            data["replace-field"] = json!(self.fields_to_replace);
            self.fields_to_add.clear();
        }

        match self.collection.client.post(&path, &data).await {
            Ok(_) => Ok(()),
            Err(_) => Err(SolrError),
        }
    }
}

#[derive(Debug)]
/// A query API
pub struct Query<'a, 'b> {
    collection: &'a Collection<'b>,
    query: Option<String>,
    // https://lucene.apache.org/solr/guide/8_5/common-query-parameters.html
    def_type: Option<String>,
    sort: Option<String>,
    start: Option<usize>,
    rows: Option<usize>,
    filter_query: Option<String>,
    field_list: Option<String>,
    debug: Option<String>,
    explain_other: Option<String>,
    time_allowed: Option<usize>,
    segment_terminate_early: Option<bool>,
    omit_header: Option<bool>,
    wt: Option<String>,
    cache: Option<bool>,
    log_params_list: Option<String>,
    echo_params: Option<String>,
}

impl<'a, 'b> Query<'a, 'b> {
    fn new(collection: &'b Collection) -> Query<'a, 'b> {
        Query {
            collection: &collection,
            query: None,
            def_type: None,
            sort: None,
            start: None,
            rows: None,
            filter_query: None,
            field_list: None,
            debug: None,
            explain_other: None,
            time_allowed: None,
            segment_terminate_early: None,
            omit_header: None,
            wt: None,
            cache: None,
            log_params_list: None,
            echo_params: None,
        }
    }

    /// # See
    /// https://lucene.apache.org/solr/guide/8_5/the-standard-query-parser.html
    pub fn query(&mut self, query: String) -> &mut Self {
        self.query = Some(query);
        self
    }

    /// # See
    /// https://lucene.apache.org/solr/guide/8_5/common-query-parameters.html#deftype-parameter
    pub fn def_type(&mut self, def_type: String) -> &mut Self {
        self.def_type = Some(def_type);
        self
    }

    /// # See
    /// https://lucene.apache.org/solr/guide/8_5/common-query-parameters.html#sort-parameter
    pub fn sort(&mut self, sort: String) -> &mut Self {
        self.sort = Some(sort);
        self
    }

    /// # See
    /// https://lucene.apache.org/solr/guide/8_5/common-query-parameters.html#start-parameter
    pub fn start(&mut self, start: usize) -> &mut Self {
        self.start = Some(start);
        self
    }

    /// # See
    /// https://lucene.apache.org/solr/guide/8_5/common-query-parameters.html#rows-parameter
    pub fn rows(&mut self, rows: usize) -> &mut Self {
        self.rows = Some(rows);
        self
    }

    /// # See
    /// https://lucene.apache.org/solr/guide/8_5/common-query-parameters.html#fq-filter-query-parameter
    pub fn filter_query(&mut self, filter_query: String) -> &mut Self {
        self.filter_query = Some(filter_query);
        self
    }

    /// # See
    /// https://lucene.apache.org/solr/guide/8_5/common-query-parameters.html#fl-field-list-parameter
    pub fn fields(&mut self, fields: String) -> &mut Self {
        self.field_list = Some(fields);
        self
    }

    /// # See
    /// https://lucene.apache.org/solr/guide/8_5/common-query-parameters.html#debug-parameter
    pub fn debug(&mut self, debug: String) -> &mut Self {
        self.debug = Some(debug);
        self
    }

    /// # See
    /// https://lucene.apache.org/solr/guide/8_5/common-query-parameters.html#explainother-parameter
    pub fn explain_other(&mut self, explain_other: String) -> &mut Self {
        self.explain_other = Some(explain_other);
        self
    }

    /// # See
    /// https://lucene.apache.org/solr/guide/8_5/common-query-parameters.html#timeallowed-parameter
    pub fn time_allowed(&mut self, time_allowed: usize) -> &mut Self {
        self.time_allowed = Some(time_allowed);
        self
    }

    /// # See
    /// https://lucene.apache.org/solr/guide/8_5/common-query-parameters.html#segmentterminateearly-parameter
    pub fn segment_terminate_early(&mut self, segment_terminate_early: bool) -> &mut Self {
        self.segment_terminate_early = Some(segment_terminate_early);
        self
    }

    /// # See
    /// https://lucene.apache.org/solr/guide/8_5/common-query-parameters.html#omitheader-parameter
    pub fn omit_header(&mut self, omit_header: bool) -> &mut Self {
        self.omit_header = Some(omit_header);
        self
    }

    /// # See
    /// https://lucene.apache.org/solr/guide/8_5/common-query-parameters.html#wt-parameter
    pub fn wt(&mut self, wt: String) -> &mut Self {
        self.wt = Some(wt);
        self
    }

    /// # See
    /// https://lucene.apache.org/solr/guide/8_5/common-query-parameters.html#cache-parameter
    pub fn cache(&mut self, cache: bool) -> &mut Self {
        self.cache = Some(cache);
        self
    }

    /// # See
    /// https://lucene.apache.org/solr/guide/8_5/common-query-parameters.html#logparamslist-parameter
    pub fn log_params_list(&mut self, log_params_list: String) -> &mut Self {
        self.log_params_list = Some(log_params_list);
        self
    }

    /// # See
    /// https://lucene.apache.org/solr/guide/8_5/common-query-parameters.html#echoparams-parameter
    pub fn echo_params(&mut self, echo_params: String) -> &mut Self {
        self.echo_params = Some(echo_params);
        self
    }

    fn build_path(&self) -> String {
        let q = self.query.as_ref().unwrap();
        let q = self.collection.client.url_encode(q);
        let mut path: String = format!("{}/select?q={}", self.collection.name, q);

        if self.def_type.is_some() {
            let temp = self.def_type.as_ref().unwrap();
            let temp = self.collection.client.url_encode(temp);
            path = format!("{}&defType={}", path, temp);
        }

        if self.sort.is_some() {
            let temp = self.sort.as_ref().unwrap();
            let temp = self.collection.client.url_encode(temp);
            path = format!("{}&sort={}", path, temp);
        }

        if self.start.is_some() {
            let temp = self.start.as_ref().unwrap();
            path = format!("{}&start={}", path, temp);
        }

        if self.rows.is_some() {
            let temp = self.rows.as_ref().unwrap();
            path = format!("{}&rows={}", path, temp);
        }

        if self.filter_query.is_some() {
            let temp = self.filter_query.as_ref().unwrap();
            let temp = self.collection.client.url_encode(temp);
            path = format!("{}&fq={}", path, temp);
        }

        if self.field_list.is_some() {
            let temp = self.field_list.as_ref().unwrap();
            let temp = self.collection.client.url_encode(temp);
            path = format!("{}&fl={}", path, temp);
        }

        if self.debug.is_some() {
            let temp = self.debug.as_ref().unwrap();
            let temp = self.collection.client.url_encode(temp);
            path = format!("{}&debug={}", path, temp);
        }

        if self.explain_other.is_some() {
            let temp = self.explain_other.as_ref().unwrap();
            let temp = self.collection.client.url_encode(temp);
            path = format!("{}&explainOther={}", path, temp);
        }

        if self.time_allowed.is_some() {
            let temp = self.time_allowed.as_ref().unwrap();
            path = format!("{}&timeAllowed={}", path, temp);
        }

        if self.segment_terminate_early.is_some() {
            let temp = self.segment_terminate_early.as_ref().unwrap();
            path = format!("{}&segmentTerminateEarly={}", path, temp);
        }

        if self.omit_header.is_some() {
            let temp = self.omit_header.as_ref().unwrap();
            path = format!("{}&omitHeader={}", path, temp);
        }

        if self.wt.is_some() {
            let temp = self.wt.as_ref().unwrap();
            let temp = self.collection.client.url_encode(temp);
            path = format!("{}&wt={}", path, temp);
        }

        if self.cache.is_some() {
            let temp = self.cache.as_ref().unwrap();
            path = format!("{}&cache={}", path, temp);
        }

        if self.log_params_list.is_some() {
            let temp = self.log_params_list.as_ref().unwrap();
            let temp = self.collection.client.url_encode(temp);
            path = format!("{}&logParamsList={}", path, temp);
        }

        if self.echo_params.is_some() {
            let temp = self.echo_params.as_ref().unwrap();
            let temp = self.collection.client.url_encode(temp);
            path = format!("{}&echoParams={}", path, temp);
        }

        path
    }

    /// Commits the query and returns its result.
    ///
    /// # Example
    /// ```
    /// let users_found = users.search()
    ///     .query("name:Some")
    ///     .sort("age asc")
    ///     .field_list("name,age")
    ///     .commit().await?;
    /// ```
    pub async fn commit(&self) -> Result<Vec<serde_json::Value>, SolrError> {
        if self.query.is_none() {
            return Err(SolrError);
        }
        let path = self.build_path();
        let res = match self.collection.client.get(&path).await {
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
