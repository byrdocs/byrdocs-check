use anyhow;
use regex;
use rusoto_core::HttpClient;
use rusoto_s3::S3;
use serde;
use serde_yaml;
use std::path::Path;
use structopt::StructOpt;
use tokio::{
    self,
    io::{AsyncReadExt, BufReader},
};

#[derive(StructOpt)]
struct Input {
    #[structopt(short = "d", long = "dir", help = "The path to check", required = true)]
    dir: String,
    #[structopt(short = "u", long = "url", help = "S3 url", required = true)]
    s3_url: String,
    #[structopt(short = "b", long = "backend", help = "Backend url", required = true)]
    backend_url: String,
    #[structopt(short = "t", long = "token", help = "Token", required = true)]
    backend_token: String,
    #[structopt(
        short = "a",
        long = "ACCESS_KEY_ID",
        help = "ACCESS_KEY_ID",
        required = true
    )]
    assess_key_id: String,
    #[structopt(
        short = "s",
        long = "SECRET_ACCESS_KEY",
        help = "SECRET_ACCESS_KEY",
        required = true
    )]
    secret_access_key: String,
    #[structopt(short = "c", long = "bucket", help = "bucket name", required = true)]
    bucket: String,
}

#[derive(serde::Deserialize, Debug)]
enum Type {
    #[serde(rename = "test")]
    Test,
    #[serde(rename = "book")]
    Book,
    #[serde(rename = "doc")]
    Doc,
}

#[derive(serde::Deserialize, Debug)]
#[serde(untagged)]
enum Data {
    Test(Test),
    Book(Book),
    Doc(Doc),
}

#[derive(serde::Deserialize, Debug)]
#[allow(dead_code)]
struct MetaData {
    id: String,
    url: String,
    #[serde(rename = "type")]
    type_: Type,
    data: Data,
}

#[derive(serde::Deserialize, Debug)]
#[allow(dead_code)]
struct Test {
    title: String,
    college: Option<String>,
    course: Course,
    filetype: String,
    stage: Option<String>,
    content: Vec<String>,
}

#[derive(serde::Deserialize, Debug)]
#[allow(dead_code)]
struct Course {
    #[serde(rename = "type")]
    type_: Option<String>,
    name: Option<String>,
}

#[derive(serde::Deserialize, Debug)]
#[allow(dead_code)]
struct Book {
    title: String,
    authors: Vec<String>,
    translators: Vec<String>,
    edition: Option<String>,
    publisher: String,
    isbn: String,
    filetype: String,
}

#[derive(serde::Deserialize, Debug)]
#[allow(dead_code)]
struct Doc {
    title: String,
    filetype: String,
    course: Course,
    content: Vec<DocContent>,
}

#[derive(serde::Deserialize, Debug)]
#[serde(rename_all = "PascalCase")]
enum DocContent {
    思维导图,
    题库,
    答案,
    知识点,
    课件,
}

#[derive(serde::Deserialize, Debug)]
#[allow(dead_code)]
struct ApiResult {
    success: bool,
    files: Vec<TempFiles>,
}

#[derive(serde::Deserialize, Debug)]
#[allow(dead_code)]
struct TempFiles {
    id: u64,
    #[serde(rename = "createdAt")]
    created_at: String,
    #[serde(rename = "fileName")]
    file_name: String,
    #[serde(rename = "fileSize")]
    file_size: u64,
    uploader: String,
    #[serde(rename = "uploadTime")]
    upload_time: String,
    status: Status,
    #[serde(rename = "errorMessage")]
    error_message: Option<String>,
}

#[derive(serde::Deserialize, Debug)]
enum Status {
    Published,
    Pending,
    Timeout,
    Expired,
    Error,
    Uploaded,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let input = Input::from_args();

    let end_point = input.s3_url;
    let http_client = HttpClient::new()?;
    let credentials = rusoto_core::credential::StaticProvider::new_minimal(
        input.assess_key_id,
        input.secret_access_key,
    );
    let region = rusoto_core::Region::Custom {
        name: "byr".to_owned(),
        endpoint: end_point,
    };
    let s3_client = rusoto_s3::S3Client::new_with(http_client, credentials, region);
    let request = rusoto_s3::ListObjectsV2Request {
        bucket: input.bucket.clone(),
        ..Default::default()
    };
    let result = s3_client.list_objects_v2(request).await?;
    let s3_file_list = result
        .contents
        .unwrap()
        .iter()
        .map(|item| item.key.clone().unwrap())
        .collect::<Vec<_>>();

    let backend_client = reqwest::Client::new();
    let temp_files = backend_client
        .get(format!("{}/api/file/notPublished", input.backend_url))
        .bearer_auth(input.backend_token)
        .send()
        .await?
        .json::<ApiResult>()
        .await?;

    let mut success_book = 0;
    let mut success_test = 0;
    let mut success_doc = 0;
    let mut total = 0;
    let dir_path = Path::new(&input.dir);
    
    for entry in dir_path.read_dir()? {
        total += 1;
        let entry = entry?;
        if !entry.file_name().to_str().unwrap().ends_with(".yml") {
            continue;
        }
        let path = entry.path();
        if path.is_file() {
            let file = std::fs::File::open(&path)?;
            let metadata: MetaData = serde_yaml::from_reader(file)?;
            let url_regex =
                regex::Regex::new(r"^https://byrdocs\.org/files/[a-fA-F0-9]{32}\.(pdf|zip)$")
                    .unwrap();
            if !url_regex.is_match(&metadata.url) | !metadata.url.contains(metadata.id.as_str()) {
                println!("Invalid URL or id: {}", path.display());
                continue;
            }
            if !s3_file_list.contains(&metadata.url[metadata.url.len() - 36..].to_string()) {
                println!("Not found in S3: {}", path.display());
                continue;
            }
            if !(path.file_name().unwrap().to_str().unwrap() == format!("{}.yml", metadata.id)) {
                println!("Invalid file name: {}", path.display());
                continue;
            }

            for temp_file in &temp_files.files {
                if temp_file.file_name.as_str() == format!("{}.pdf", metadata.id)
                    || temp_file.file_name.as_str() == format!("{}.zip", metadata.id)
                {
                    match temp_file.status {
                        Status::Published => {
                            println!("Published: {}", path.display());
                        }
                        _ => {
                            let request = rusoto_s3::GetObjectRequest {
                                bucket: input.bucket.clone(),
                                key: metadata.url[metadata.url.len() - 36..].to_string(),
                                ..Default::default()
                            };
                            let file = s3_client.get_object(request).await?.body.unwrap();
                            let mut reader = BufReader::new(file.into_async_read());
                            let mut buffer = Vec::new();
                            reader.read_to_end(&mut buffer).await?;
                            let md5 = md5::compute(&buffer);
                            if !(format!("{:x}", md5) == metadata.id) {
                                println!("MD5 not match: {}", path.display());
                            }
                        }
                    }
                    break;
                }
            }
            match metadata.data {
                Data::Test(_test) => {
                    success_test += 1;
                }
                Data::Book(_book) => {
                    success_book += 1;
                }
                Data::Doc(_doc) => {
                    success_doc += 1;
                }
            }
        }
    }
    println!(
        "Total: {}, Success: {}, Book: {}, Test: {}, Doc: {}",
        total,
        success_book + success_test + success_doc,
        success_book,
        success_test,
        success_doc
    );
    if total != success_book + success_test + success_doc {
        return Err(anyhow::anyhow!("Some files are invalid"));
    }
    Ok(())
}
