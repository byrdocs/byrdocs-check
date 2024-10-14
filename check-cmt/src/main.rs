use std::{collections::HashSet,path::Path};
use rusoto_core::HttpClient;
use rusoto_s3::{Object, S3Client, S3};
use serde_json::json;
use tokio::{self, io::{AsyncReadExt, AsyncWriteExt, BufReader}};
use serde_yaml;
use serde;
use anyhow;
use structopt::StructOpt;
use tokio::fs::File;
use pdfium_render::prelude::*;
use webp;
use webp::Encoder;


#[derive(serde::Deserialize, serde::Serialize)]
enum Type {
    #[serde(rename = "test")]
    Test,
    #[serde(rename = "book")]
    Book,
    #[serde(rename = "doc")]
    Doc,
}

#[derive(serde::Deserialize, serde::Serialize)]
#[serde(untagged)]
enum Data {
    Test(Test),
    Book(Book),
    Doc(Doc),
    
}

#[derive(serde::Deserialize, serde::Serialize)]
#[allow(dead_code)]
struct MetaData {
    id: String,
    url: String,
    #[serde(rename = "type")]
    type_: Type,
    data: Data
}

#[derive(serde::Deserialize, serde::Serialize)]
#[allow(dead_code)]
struct Test {
    title: String,
    college: Option<String>,
    course: Course,
    filetype: String,
    stage: Option<String>,
    content: Vec<String>,
    filesize: Option<u64>,
}

#[derive(serde::Deserialize, serde::Serialize)]
#[allow(dead_code)]
struct Course {
    #[serde(rename = "type")]
    type_: Option<String>,
    name: Option<String>,
}

#[derive(serde::Deserialize, serde::Serialize)]
#[allow(dead_code)]
struct Book {
    title: String,
    authors: Vec<String>,
    translators: Vec<String>,
    edition: Option<String>,
    publisher: String,
    isbn: String,
    filetype: String,
    isbn_raw: Option<String>,
    filesize: Option<u64>,
}

#[derive(serde::Deserialize, serde::Serialize)]
#[allow(dead_code)]
struct Doc {
    title: String,
    filetype: String,
    course: Course,
    content: Vec<DocContent>,
    filesize: Option<u64>,
}

#[derive(serde::Deserialize, serde::Serialize)]
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
    file_size: Option<u64>,
    uploader: String,
    #[serde(rename = "uploadTime")]
    upload_time: Option<String>,
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

#[derive(StructOpt)]
struct Input {
    #[structopt(short = "d", long = "dir", help = "The metadata path to check", required = true)]
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
    #[structopt(long = "r2_url", help = "R2 url", required = true)]
    r2_url: String,
    #[structopt(long = "r2_acc", help = "R2 ACCESS_KEY_ID", required = true)]
    r2_access_key_id: String,
    #[structopt(long = "r2_secret", help = "R2 SECRET", required = true)]
    r2_secret_access_key: String,
    #[structopt(long = "r2_bucket", help = "R2 bucket", required = true)]
    r2_bucket: String,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let input = Input::from_args();

    if !Path::new(&input.dir).exists() {
        eprintln!("The metadata directory does not exist: {}", input.dir);
        std::process::exit(1);
    }

    let s3_client = get_s3_client(input.s3_url, input.assess_key_id, input.secret_access_key).await?;
    let s3_obj = s3_client.list_objects_v2(rusoto_s3::ListObjectsV2Request {
            bucket: input.bucket.clone(),
            ..Default::default()
        }).await?.contents.unwrap();
    let files_need_update = get_files_need_update(&s3_obj).await?;
    
    std::fs::create_dir_all("./tmp1")?;
    std::fs::create_dir_all("./tmp2")?;
    std::fs::create_dir_all("./tmp3")?;
    download_files(&s3_client, &s3_obj, files_need_update, input.bucket.clone()).await?;

    generate_jpg_files().await?;

    generate_webp_files().await?;

    upload_files(&s3_client, input.bucket).await?;

    //img part over

    publish_files(input.backend_url, input.backend_token, &input.dir).await?;

    merge_json(&input.dir, &s3_obj).await?;

    upload_metadata(input.r2_url, input.r2_access_key_id, input.r2_secret_access_key, input.r2_bucket, &input.dir).await?;

    Ok(())
}

async fn get_s3_client(s3_url: String, access_key: String, secret_key: String) -> anyhow::Result<S3Client> {
    let end_point = s3_url;
    let http_client = HttpClient::new()?;
    let credentials = rusoto_core::credential::StaticProvider::new_minimal(access_key, secret_key);
    let region = rusoto_core::Region::Custom {
        name: "apac".to_owned(),
        endpoint: end_point,
    };
    let s3_client = rusoto_s3::S3Client::new_with(http_client, credentials, region);
    Ok(s3_client)
}

async fn get_files_need_update(s3_obj: &Vec<Object>) -> anyhow::Result<HashSet<String>> {
    
    let mut jpg_files = HashSet::new();
    let mut webp_files = HashSet::new();
    for item in s3_obj {
        let key = item.key.clone().unwrap();
        if key.ends_with(".jpg") {
            jpg_files.insert(key[..32].to_string());
        }
        if key.ends_with(".webp") {
            webp_files.insert(key[..32].to_string());
        }
    }
    println!("Jpg files: {:?}", jpg_files);
    println!("Webp files: {:?}", webp_files);
    let mut raw_files = HashSet::new();
    for item in s3_obj {
        let key = item.key.clone().unwrap_or_default();
        if key.ends_with(".pdf") || key.ends_with(".zip") {
            raw_files.insert(key[..32].to_string());
        }
    }
    println!("Raw files: {:?}", raw_files);
    let jpg_diff = raw_files.difference(&jpg_files).cloned().collect::<HashSet<_>>();
    let webp_diff = raw_files.difference(&webp_files).cloned().collect::<HashSet<_>>();
    let files_need_update = jpg_diff.union(&webp_diff).cloned().collect::<HashSet<_>>();
    //Get files need to update
    println!("Files need to update: {:?}", files_need_update);
    Ok(files_need_update)
}

async fn download_files(s3_client: &S3Client, s3_obj: &Vec<Object>, files_need_update: HashSet<String>, bucket: String) -> anyhow::Result<()> {
    std::fs::create_dir_all("./tmp1")?;
    let dir = Path::new("./tmp1");
    for file in s3_obj {
        let key = file.key.clone().unwrap();
        if files_need_update.contains(&key[..32].to_string()) && key.ends_with(".pdf") {
            let request = rusoto_s3::GetObjectRequest {
                bucket: bucket.clone(),
                key: key.clone(),
                ..Default::default()
            };
            let result = s3_client.get_object(request).await?;
            let body = result.body.unwrap();
            let mut reader = BufReader::new(body.into_async_read());
            let mut buf = Vec::new();
            reader.read_to_end(&mut buf).await?;
            let file_path = dir.join(&key);
            let mut file = File::create(file_path).await?;
            file.write_all(&buf).await?;
        }
    }//Download files
    println!("Files downloaded");
    Ok(())
}

async fn generate_jpg_files() -> anyhow::Result<()> {
    let dir = Path::new("./tmp1");
    let pdfium = Pdfium::new(
        Pdfium::bind_to_library(Pdfium::pdfium_platform_library_name_at_path("./lib"))
            .or_else(|_| Pdfium::bind_to_system_library())?,
    );
    for file in dir.read_dir()? {
        let file = file?;
        let path = file.path();
        if path.is_file() && path.extension().and_then(|s| s.to_str()) == Some("pdf") {
            println!("Processing pdf: {:?}", path.to_string_lossy().as_ref());
            if let Ok(document) = pdfium.load_pdf_from_file(path.to_str().unwrap(), None) {
                let render_config = PdfRenderConfig::new()
                    .set_target_width(2000)
                    .set_maximum_height(2000)
                    .rotate_if_landscape(PdfPageRenderRotation::Degrees90, true);
                let document = document.pages().get(0).unwrap()
                    .render_with_config(&render_config)?
                    .as_image()
                    .into_rgb8();
                document.save_with_format(
                        format!("./tmp2/{}.jpg", path.file_name().unwrap().to_str().unwrap()[..32].to_string()), 
                        image::ImageFormat::Jpeg
                )?;
            }else{
                println!("Failed to load pdf: {:?}", path.to_string_lossy().as_ref());
            };
        }
    }//Generate jpg files
    println!("jpg Files generated");
    Ok(())
}

async fn generate_webp_files() -> anyhow::Result<()> {
    let dir = Path::new("./tmp2");
    for file in dir.read_dir()? {
        let file = file?;
        let path = file.path();
        if path.is_file() && path.extension().and_then(|s| s.to_str()) == Some("jpg") {
            let img = image::open(&path)?.to_rgb8();
            let dynamic_img = image::DynamicImage::ImageRgb8(img);
            let encoder = Encoder::from_image(&dynamic_img).unwrap();
            let mut webp_data = encoder.encode(100.0);
            if webp_data.len() <= 50 * 1024 {
                let file_path = Path::new("./tmp3").join(format!("{}.webp", path.file_stem().unwrap().to_str().unwrap()));
                let mut file = File::create(file_path).await?;
                file.write_all(&webp_data).await?;
                return Ok(());
            }
            let quality = ((48.0*1024.0 / webp_data.len() as f32) * 100.0) as i32;
            webp_data = encoder.encode(std::cmp::max(quality, 5) as f32);
    
            let file_path = Path::new("./tmp3").join(format!("{}.webp", path.file_stem().unwrap().to_str().unwrap()));
            let mut file = File::create(file_path.clone()).await?;
            println!("file: {:#?}", file_path.to_string_lossy());
            file.write_all(&webp_data).await?;
        }
    }//Generate jpg files
    println!("webp Files generated");
    Ok(())
}

async fn upload_files(s3_client: &S3Client, bucket: String) -> anyhow::Result<()> {
    let dir = Path::new("./tmp2");
    for file in dir.read_dir()? {
        let file = file?;
        let path = file.path();
        if path.is_file() && path.extension().and_then(|s| s.to_str()) == Some("jpg") {
            let file = File::open(&path).await?;
            let mut reader = BufReader::new(file);
            let mut buf = Vec::new();
            reader.read_to_end(&mut buf).await?;
            let request = rusoto_s3::PutObjectRequest {
                bucket: bucket.clone(),
                key: path.file_name().unwrap().to_string_lossy().as_ref().to_string(),
                body: Some(buf.into()),
                ..Default::default()
            };
            s3_client.put_object(request).await?;
        }
    }
    let dir = Path::new("./tmp3");
    for file in dir.read_dir()? {
        let file = file?;
        let path = file.path();
        if path.is_file() && path.extension().and_then(|s| s.to_str()) == Some("webp") {
            let file = File::open(&path).await?;
            let mut reader = BufReader::new(file);
            let mut buf = Vec::new();
            reader.read_to_end(&mut buf).await?;
            let request = rusoto_s3::PutObjectRequest {
                bucket: bucket.clone(),
                key: path.file_name().unwrap().to_string_lossy().as_ref().to_string(),
                body: Some(buf.into()),
                ..Default::default()
            };
            s3_client.put_object(request).await?;
        }
    }
    //Upload files
    println!("Files uploaded");
    Ok(())
}

async fn publish_files(backend_url: String, backend_token: String, dir: &String) -> anyhow::Result<()> {
    
    let path = Path::new(dir);
    let mut local_files = HashSet::new();
    for file in path.read_dir()?{
        let file = file?;
        let path = file.path();
        if path.is_file() && path.extension().and_then(|s| s.to_str()) == Some("pdf") {
            let file_name = path.file_name().unwrap().to_string_lossy().as_ref().to_string();
            local_files.insert(file_name[..32].to_string());
        }
    };

    let backend_client = reqwest::Client::new();
    let temp_files = backend_client.get(format!("{}/api/file/notPublished", backend_url))
        .bearer_auth(backend_token.clone())
        .send()
        .await?
        .json::<ApiResult>()
        .await?;
    let temp_filename = temp_files.files.iter().map(|file| file.file_name[..32].to_string()).collect::<HashSet<_>>();
    let publish_list = local_files.intersection(&temp_filename).cloned().collect::<HashSet<_>>();
    let mut ids = Vec::new();
    for file in temp_files.files {
        if publish_list.contains(&file.file_name[..32].to_string()) {
            ids.push(file.id);
        }
    }
    backend_client.post(format!("{}/api/file/publish", backend_url))
        .bearer_auth(backend_token)
        .json(&json!({
            "ids": ids
        }))
        .send().await?;
    //Publish files
    println!("Files published");
    Ok(())
}

async fn merge_json(dir: &String, s3_obj: &Vec<Object>) -> anyhow::Result<()> {
    let dir = Path::new(dir.as_str());
    let mut json = Vec::new();
    for metadata in dir.read_dir()? {
        let metadata = metadata?;
        let path = metadata.path();
        if path.is_file() && path.extension().and_then(|s| s.to_str()) == Some("yml") {
            let file = File::open(&path).await?;
            let mut reader = BufReader::new(file);
            let mut buf = Vec::new();
            reader.read_to_end(&mut buf).await?;
            let mut metadata: MetaData = serde_yaml::from_slice(&buf)?;

            if let Data::Book(ref mut book) = metadata.data {
                let key = format!("{}.pdf", metadata.id);
                let file = s3_obj.iter().find(|file| file.key.clone().unwrap() == key);
                if let Some(file) = file {
                    book.filesize = Some(file.size.unwrap() as u64);
                }
                book.isbn_raw = Some(book.isbn.replace("-", ""));
            }

            if let Data::Doc(ref mut doc) = metadata.data {
                let key = format!("{}.pdf", metadata.id);
                let file = s3_obj.iter().find(|file| file.key.clone().unwrap() == key);
                if let Some(file) = file {
                    doc.content.push(DocContent::课件);
                    doc.filesize = Some(file.size.unwrap() as u64);
                }
            }

            if let Data::Test(ref mut test) = metadata.data {
                let key = format!("{}.pdf", metadata.id);
                let file = s3_obj.iter().find(|file| file.key.clone().unwrap() == key);
                if let Some(file) = file {
                    test.filesize = Some(file.size.unwrap() as u64);
                }
            }

            json.push(metadata);
        }            
    }
    let temp_file_path = dir.join("metadata2.json");
    let mut temp_file = File::create(&temp_file_path).await?;
    let json_data = serde_json::to_string_pretty(&json)?;
    temp_file.write_all(json_data.as_bytes()).await?;
    println!("Metadata JSON written to: {:?}", temp_file_path);
    //merge to json
    Ok(())
}

async fn upload_metadata(r2_url: String, r2_access_key_id: String, r2_secret_access_key: String, r2_bucket: String, dir: &String) -> anyhow::Result<()> {
    let r2_client = get_s3_client(r2_url, r2_access_key_id, r2_secret_access_key).await?;
    let metadata_json = File::open(Path::new(&dir).join("metadata2.json")).await?;
    let mut reader = BufReader::new(metadata_json);
    let mut buf = Vec::new();
    reader.read_to_end(&mut buf).await?;
    let request = rusoto_s3::PutObjectRequest {
        bucket: r2_bucket.clone(),
        key: "metadata2.json".to_string(),
        body: Some(buf.into()),
        ..Default::default()
    };
    r2_client.put_object(request).await?;
    println!("Uploading metadata to R2");
    Ok(())
}