use std::{clone, path::Path};
use rusoto_core::HttpClient;
use rusoto_s3::S3;
use tokio;

use rusoto;
use regex;
use serde_yaml;
use serde;
use anyhow;
use structopt::StructOpt;

#[derive(StructOpt)]
struct Input {
    #[structopt(short = "d", long = "dir", help = "The path to check", required = true)]
    dir: String,
    #[structopt(short = "u", long = "url", help = "S3 url", required = true)]
    s3_url: String,
    // #[structopt(short = "b", long = "backend", help = "Backend url", required = true)]
    // backend_url: String,
    #[structopt(short = "a", long = "ACCESS_KEY_ID", help = "ACCESS_KEY_ID", required = true)]
    assess_key_id: String,
    #[structopt(short = "s", long = "SECRET_ACCESS_KEY", help = "SECRET_ACCESS_KEY", required = true)]
    secret_access_key: String,
    #[structopt(short = "c", long = "bucket", help = "bucket name", required = true)]
    bucket: String,
}

#[derive(serde::Deserialize)]
enum Type {
    #[serde(rename = "test")]
    Test,
    #[serde(rename = "book")]
    Book,
    #[serde(rename = "doc")]
    Doc,
}

#[derive(serde::Deserialize)]
#[serde(untagged)]
enum Data {
    Test(Test),
    Book(Book),
    Doc(Doc),
    
}

#[derive(serde::Deserialize)]
struct MetaData {
    id: String,
    url: String,
    #[serde(rename = "type")]
    type_: Type,
    data: Data
}

#[derive(serde::Deserialize)]
struct Test {
    title: String,
    college: Option<String>,
    course: Course,
    filetype: String,
    stage: Option<String>,
    content: Vec<String>,
}

#[derive(serde::Deserialize)]
struct Course {
    #[serde(rename = "type")]
    type_: Option<String>,
    name: Option<String>,
}

#[derive(serde::Deserialize)]
struct Book {
    title: String,
    authors: Vec<String>,
    translators: Vec<String>,
    edition: Option<String>,
    publisher: String,
    isbn: String,
    filetype: String,
}

#[derive(serde::Deserialize)]
struct Doc {
    title: String,
    filetype: String,
    course: Course,
    content: Vec<DocContent>,
}

#[derive(serde::Deserialize)]
#[serde(rename_all = "PascalCase")]
enum DocContent {
    思维导图,
    题库,
    答案,
    知识点,
    课件,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let input = Input::from_args();

    let end_point = input.s3_url;
    let http_client = HttpClient::new()?;
    let credentials = rusoto_core::credential::StaticProvider::new_minimal(input.assess_key_id, input.secret_access_key);
    let region = rusoto_core::Region::Custom {
        name: "byr".to_owned(),
        endpoint: end_point,
    };
    let client = rusoto_s3::S3Client::new_with(http_client, credentials, region);
    let request = rusoto_s3::ListObjectsV2Request {
        bucket: input.bucket,
        ..Default::default()
    };
    let result = client.list_objects_v2(request).await?;
    println!("{:#?}", result.contents);

    let dir_path = Path::new(&input.dir);
    for entry in dir_path.read_dir()? {
        let entry = entry?;
        let path = entry.path();
        if path.is_file() {
            let file = std::fs::File::open(&path)?;
            let metadata: MetaData = serde_yaml::from_reader(file)?;
            let url_regex = regex::Regex::new(r"^https://byrdocs\.org/files/[a-fA-F0-9]{32}\.(pdf|zip)$").unwrap();
            if !url_regex.is_match(&metadata.url) {
                println!("Invalid URL: {}", path.display());
                continue;
            }
            match metadata.data {
                Data::Test(test) => {
                    println!("Test: {}", test.title);
                }
                Data::Book(book) => {
                    println!("Book: {}", book.title);
                }
                Data::Doc(doc) => {
                    println!("Doc: {}", doc.title);
                }
            }
        }
    }


    Ok(())
}