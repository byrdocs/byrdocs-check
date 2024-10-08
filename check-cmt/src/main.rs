use std::{collections::HashSet,path::Path};
use rusoto_core::HttpClient;
use rusoto_s3::S3;
use tokio::{self, io::{AsyncReadExt, BufReader}};
use regex;
use serde_yaml;
use serde;
use anyhow;
use structopt::StructOpt;
use std::fs::File;
use std::io::Write;
use tempfile::tempdir;

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
    #[structopt(short = "a", long = "ACCESS_KEY_ID", help = "ACCESS_KEY_ID", required = true)]
    assess_key_id: String,
    #[structopt(short = "s", long = "SECRET_ACCESS_KEY", help = "SECRET_ACCESS_KEY", required = true)]
    secret_access_key: String,
    #[structopt(short = "c", long = "bucket", help = "bucket name", required = true)]
    bucket: String,
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
    let s3_client = rusoto_s3::S3Client::new_with(http_client, credentials, region);
    let request = rusoto_s3::ListObjectsV2Request {
        bucket: input.bucket.clone(),
        ..Default::default()
    };
    let result = s3_client.list_objects_v2(request).await?;
    let mut img_files = HashSet::new();
    for item in result.contents.clone().unwrap_or_default() {
        let key = item.key.unwrap_or_default();
        if key.ends_with(".jpg") || key.ends_with(".webp") {
            img_files.insert(key[32..].to_string());
        }
    }
    let all_files = result.clone()
        .contents.unwrap_or_default()
        .iter()
        .map(
            |item| 
            item.key.clone().unwrap_or_default()[32..].to_string()
        ).collect::<HashSet<String>>();
    let files_need_update = all_files.difference(&img_files).collect::<HashSet<_>>();
    
    std::fs::create_dir_all("./tmp")?;
    let dir = Path::new("./tmp");
    for file in result.contents.unwrap_or_default() {
        let key = file.key.unwrap_or_default();
        if files_need_update.contains(&key[32..].to_string()) && (key.ends_with(".pdf") | key.ends_with(".zip")) {
            let request = rusoto_s3::GetObjectRequest {
                bucket: input.bucket.clone(),
                key: key.clone(),
                ..Default::default()
            };
            let result = s3_client.get_object(request).await?;
            let body = result.body.unwrap();
            let mut reader = BufReader::new(body.into_async_read());
            let mut buf = Vec::new();
            reader.read_to_end(&mut buf).await?;
            let file_path = dir.join(&key);
            let mut file = File::create(file_path)?;
            file.write_all(&buf)?;
        }
    }
    Ok(())
}