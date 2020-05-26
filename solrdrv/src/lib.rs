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
    pub port: u16
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
                    0..=47 | 58..=64 | 91..=96 | 123..=MAX_CHAR_VAL => {
                        ch.encode_utf8(&mut buff);
                        buff[0..ch.len_utf8()].iter().map(|&byte| format!("%{:X}", byte)).collect::<String>()
                    }
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

    pub fn collections(&self) -> CollectionsAPI {
        CollectionsAPI::new(&self)
    }
}

#[derive(Debug)]
pub struct CollectionsAPI<'a> {
    client: &'a Solr
}

impl<'a> CollectionsAPI<'a> {
    fn new(client: &'a Solr) -> CollectionsAPI<'a> {
        CollectionsAPI {
            client: &client
        }
    }

    pub fn create(&self, name: String) -> CollectionBuilder<'a> {
        let mut builder = CollectionBuilder::new(&self.client);
        builder.name(name);
        builder
    }

    pub async fn list(&self) -> Result<Vec<Collection<'_>>, SolrError> {
        let path = String::from("admin/collections?action=LIST");
        let res = match self.client.fetch(&path).await {
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

    pub async fn get(&self, name: String) -> Result<Collection<'_>, SolrError> {
        let path = String::from(format!("admin/collections?action=LIST"));
        let res = match self.client.fetch(&path).await {
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

    pub async fn delete(&self, name: &String) -> Result<(), SolrError> {
        let path = String::from(format!("admin/collections?action=DELETE&name={}", name));
        match self.client.fetch(&path).await {
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
pub struct FieldBuilder {
    name: String,
    typename: String,
    // Source: https://lucene.apache.org/solr/guide/8_5/field-type-definitions-and-properties.html
    stored: bool,
    indexed: bool,
    doc_values: bool,
    sort_missing_first: bool,
    sort_missing_last: bool,
    multi_valued: bool,
    omit_norms: Option<bool>,
    omit_term_freq_and_positions: Option<bool>,
    omit_positions: Option<bool>,
    uninvertible: bool,
    term_vectors: bool,
    term_positions: bool,
    term_offsets: bool,
    term_payloads: bool,
    required: bool,
    use_doc_values_as_stored: bool,
    large: bool,
}

impl FieldBuilder {
    pub fn new(name: String) -> FieldBuilder {
        FieldBuilder {
            name: name,
            typename: "".into(),
            stored: true,
            indexed: true,
            doc_values: true,
            sort_missing_first: false,
            sort_missing_last: false,
            multi_valued: false,
            omit_norms: None,
            omit_term_freq_and_positions: None,
            omit_positions: None,
            uninvertible: true,
            term_vectors: false,
            term_positions: false,
            term_offsets: false,
            term_payloads: false,
            required: false,
            use_doc_values_as_stored: true,
            large: false,
        }
    }

    pub fn build(&self) -> Result<serde_json::Value, SolrError> {
        if self.typename.len() == 0 {
            return Err(SolrError);
        }

        let mut json = json!({
            "name": self.name,
            "type": self.typename,
            "stored": self.stored,
            "indexed": self.indexed,
            "docValues": self.doc_values,
            "sortMissingFirst": self.sort_missing_first,
            "sortMissingLast": self.sort_missing_last,
            "multiValued": self.multi_valued,
            "uninvertible": self.uninvertible,
            "termVectors": self.term_vectors,
            "termPositions": self.term_positions,
            "termOffsets": self.term_offsets,
            "termPayloads": self.term_payloads,
            "required": self.required,
            "useDocValuesAsStored": self.use_doc_values_as_stored,
            "large": self.large
        });

        if self.omit_norms.is_some() {
            json["omitNorms"] = serde_json::Value::Bool(self.omit_norms.unwrap());
        }

        if self.omit_term_freq_and_positions.is_some() {
            json["omitTermFreqAndPositions"] = serde_json::Value::Bool(self.omit_term_freq_and_positions.unwrap());
        }

        if self.omit_positions.is_some() {
            json["omitPositions"] = serde_json::Value::Bool(self.omit_positions.unwrap());
        }

        Ok(json)
    }

    pub fn set_type(&mut self, val: String) -> &mut Self {
        self.typename = val;
        self
    }

    pub fn set_stored(&mut self, val: bool) -> &mut Self {
        self.stored = val;
        self
    }

    pub fn set_indexed(&mut self, val: bool) -> &mut Self {
        self.indexed = val;
        self
    }

    pub fn set_doc_values(&mut self, val: bool) -> &mut Self {
        self.doc_values = val;
        self
    }

    pub fn set_sort_missing_first(&mut self, val: bool) -> &mut Self {
        self.sort_missing_first = val;
        self
    }

    pub fn set_sort_missing_last(&mut self, val: bool) -> &mut Self {
        self.sort_missing_last = val;
        self
    }

    pub fn set_multi_valued(&mut self, val: bool) -> &mut Self {
        self.multi_valued = val;
        self
    }

    pub fn set_omit_norms(&mut self, val: bool) -> &mut Self {
        self.omit_norms = Some(val);
        self
    }

    pub fn set_omit_term_freq_and_positions(&mut self, val: bool) -> &mut Self {
        self.omit_term_freq_and_positions = Some(val);
        self
    }

    pub fn set_omit_positions(&mut self, val: bool) -> &mut Self {
        self.omit_positions = Some(val);
        self
    }

    pub fn set_uninvertible(&mut self, val: bool) -> &mut Self {
        self.uninvertible = val;
        self
    }

    pub fn set_term_vectors(&mut self, val: bool) -> &mut Self {
        self.term_vectors = val;
        self
    }

    pub fn set_term_positions(&mut self, val: bool) -> &mut Self {
        self.term_positions = val;
        self
    }

    pub fn set_term_offsets(&mut self, val: bool) -> &mut Self {
        self.term_offsets = val;
        self
    }

    pub fn set_term_payloads(&mut self, val: bool) -> &mut Self {
        self.term_payloads = val;
        self
    }

    pub fn set_required(&mut self, val: bool) -> &mut Self {
        self.required = val;
        self
    }

    pub fn set_use_doc_values_as_stored(&mut self, val: bool) -> &mut Self {
        self.use_doc_values_as_stored = val;
        self
    }

    pub fn set_large(&mut self, val: bool) -> &mut Self {
        self.large = val;
        self
    }

    pub fn text(name: String) -> serde_json::Value {
        FieldBuilder::new(name)
            .set_type("lowercase".into())
            .build().unwrap()
    }

    pub fn string(name: String) -> serde_json::Value {
        FieldBuilder::new(name)
            .set_type("string".into())
            .set_omit_norms(true)
            .build().unwrap()
    }

    pub fn multi_string(name: String) -> serde_json::Value {
        FieldBuilder::new(name)
            .set_type("strings".into())
            .set_omit_norms(true)
            .set_multi_valued(true)
            .build().unwrap()
    }

    pub fn numeric(name: String) -> serde_json::Value {
        FieldBuilder::new(name)
            .set_type("pfloat".into())
            .build().unwrap()
    }

    pub fn double(name: String) -> serde_json::Value {
        FieldBuilder::new(name)
            .set_type("pdouble".into())
            .build().unwrap()
    }

    pub fn long(name: String) -> serde_json::Value {
        FieldBuilder::new(name)
            .set_type("plong".into())
            .build().unwrap()
    }

    pub fn fulltext(name: String) -> serde_json::Value {
        FieldBuilder::new(name)
            .set_type("text_general".into())
            .build().unwrap()
    }

    pub fn tag(name: String) -> serde_json::Value {
        FieldBuilder::new(name)
            .set_type("delimited_payloads_string".into())
            .build().unwrap()
    }

    pub fn date(name: String) -> serde_json::Value {
        FieldBuilder::new(name)
            .set_type("pdate".into())
            .build().unwrap()
    }
}

#[derive(Debug)]
pub struct CollectionBuilder<'a> {
    client: &'a Solr,
    // Source: https://lucene.apache.org/solr/guide/8_5/collection-management.html#create
    name: String,
    num_shards: Option<usize>,
    max_shards_per_node: Option<usize>,
    router_field: Option<String>,
    fields: Vec<serde_json::Value>,
}

impl<'a> CollectionBuilder<'a> {
    fn new<'b: 'a>(client: &'b Solr) -> CollectionBuilder<'a> {
        CollectionBuilder {
            client: &client,
            name: "".into(),
            num_shards: None,
            max_shards_per_node: None,
            router_field: None,
            fields: vec![],
        }
    }

    pub fn name(&mut self, name: String) -> &mut Self {
        self.name = name;
        self
    }

    pub fn num_shards(&mut self, num_shards: usize) -> &mut Self {
        self.num_shards = Some(num_shards);
        self
    }

    pub fn max_shards_per_node(&mut self, max_shards_per_node: usize) -> &mut Self {
        self.max_shards_per_node = Some(max_shards_per_node);
        self
    }

    pub fn router_field(&mut self, router_field: String) -> &mut Self {
        self.router_field = Some(router_field);
        self
    }

    pub fn field(&mut self, field: serde_json::Value) -> &mut Self {
        self.fields.push(field);
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

    pub async fn commit(&mut self) -> Result<Collection<'a>, SolrError> {
        if self.name.len() == 0 {
            return Err(SolrError);
        }

        let path = self.build_path();

        let res = match self.client.fetch(&path).await {
            Ok(r) => r,
            Err(_) => return Err(SolrError),
        };

        if res.get("success").is_none() {
            return Err(SolrError);
        }

        let col = Collection::new(&self.client, self.name.clone());
        let path = format!("{}/schema", self.name);

        // TODO: Check if the scheme was created!
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
    echo_params: Option<String>
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
            echo_params: None
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

    pub async fn commit(&self) -> Result<Vec<serde_json::Value>, SolrError> {
        if self.query.is_none() {
            return Err(SolrError);
        }
        let path = self.build_path();
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
