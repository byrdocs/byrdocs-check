mod metadata;

use pdfium_render::prelude::{PdfPageRenderRotation, PdfRenderConfig, Pdfium};
use rusoto_core::HttpClient;
use rusoto_s3::{Object, S3Client, S3};
use serde_json::{json, to_string_pretty};
use std::{collections::HashSet, path::Path};
use structopt::StructOpt;
use tokio::fs::File;
use tokio::{
    self,
    io::{AsyncReadExt, AsyncWriteExt, BufReader},
};
use webp::Encoder;

use metadata::*;

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
    #[structopt(
        short = "d",
        long = "dir",
        help = "The metadata path to check",
        required = true
    )]
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

    let s3_client =
        get_s3_client(input.s3_url, input.assess_key_id, input.secret_access_key).await?;
    let s3_obj = list_all_objects(&s3_client, &input.bucket).await;
    let mut temp_files = get_temp_files(&input.backend_url, &input.backend_token).await?;

    let publish_list = get_publish_list(&input.dir, &mut temp_files).await?; // get publish list and remove published files from temp_files

    //image part

    let files_need_update = get_files_need_update(&s3_obj, &temp_files).await?;

    std::fs::create_dir_all("./tmp1")?;
    std::fs::create_dir_all("./tmp2")?;
    std::fs::create_dir_all("./tmp3")?;
    download_files(&s3_client, &s3_obj, files_need_update, input.bucket.clone()).await?;

    generate_jpg_files().await?;

    generate_webp_files().await?;

    upload_files(&s3_client, input.bucket).await?;

    //image part over

    publish_files(
        publish_list,
        input.backend_url,
        input.backend_token,
        &mut temp_files,
    )
    .await?; // publish files

    merge_json(&input.dir, &s3_obj).await?;

    upload_metadata(
        input.r2_url,
        input.r2_access_key_id,
        input.r2_secret_access_key,
        input.r2_bucket,
        &input.dir,
    )
    .await?;

    println!("All done");

    Ok(())
}

async fn get_s3_client(
    s3_url: String,
    access_key: String,
    secret_key: String,
) -> anyhow::Result<S3Client> {
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

async fn get_files_need_update(
    s3_obj: &Vec<Object>,
    temp_files: &ApiResult,
) -> anyhow::Result<HashSet<String>> {
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

    let mut raw_files = HashSet::new();
    for item in s3_obj {
        let key = item.key.clone().unwrap_or_default();
        if key.ends_with(".pdf") && !temp_files.files.iter().any(|file| file.file_name == key) {
            raw_files.insert(key[..32].to_string());
        }
    }

    let jpg_diff = raw_files
        .difference(&jpg_files)
        .cloned()
        .collect::<HashSet<_>>();
    let webp_diff = raw_files
        .difference(&webp_files)
        .cloned()
        .collect::<HashSet<_>>();
    let files_need_update = jpg_diff.union(&webp_diff).cloned().collect::<HashSet<_>>();
    //Get files need to update
    println!("Files need to update: {:#?}", files_need_update);
    Ok(files_need_update)
}

async fn download_files(
    s3_client: &S3Client,
    s3_obj: &Vec<Object>,
    files_need_update: HashSet<String>,
    bucket: String,
) -> anyhow::Result<()> {
    std::fs::create_dir_all("./tmp1")?;
    let dir = Path::new("./tmp1");
    for file in s3_obj {
        let key = file.key.clone().unwrap();
        if files_need_update.contains(&key[..32]) && key.ends_with(".pdf") {
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
    } //Download files
    println!("Files downloaded");
    Ok(())
}

async fn generate_jpg_files() -> anyhow::Result<()> {
    let dir = Path::new("./tmp1");
    let pdfium = Pdfium::new(
        Pdfium::bind_to_library(Pdfium::pdfium_platform_library_name_at_path("./lib"))
            .or_else(|_| Pdfium::bind_to_system_library())?,
    );
    let mut error_count = 0;
    for file in dir.read_dir()? {
        let file = file?;
        let path = file.path();
        if path.is_file() && path.extension().and_then(|s| s.to_str()) == Some("pdf") {
            println!("Processing pdf: {:?}", path.to_string_lossy().as_ref());
            let mut reader = BufReader::new(File::open(&path).await?);
            let mut context = md5::Context::new();
            let mut buffer = [0; 8192];
            loop {
                let bytes_read = reader.read(&mut buffer).await?;
                if bytes_read == 0 {
                    break;
                }
                context.consume(&buffer[..bytes_read]);
            }
            let md5 = context.compute();
            if !(format!("{:x}", md5) == path.file_stem().unwrap().to_str().unwrap()) {
                println!("MD5 mismatch: {:x}", md5);
                error_count += 1;
                continue;
            }
            match pdfium.load_pdf_from_file(path.to_str().unwrap(), None) {
                Ok(document) => {
                    let render_config = PdfRenderConfig::new()
                        .set_target_width(2000)
                        .set_maximum_height(2000)
                        .rotate_if_landscape(PdfPageRenderRotation::Degrees90, true);
                    let document = document
                        .pages()
                        .get(0)
                        .unwrap()
                        .render_with_config(&render_config)?
                        .as_image()
                        .into_rgb8();
                    document.save_with_format(
                        format!(
                            "./tmp2/{}.jpg",
                            &path.file_name().unwrap().to_str().unwrap()[..32]
                        ),
                        image::ImageFormat::Jpeg,
                    )?;
                }
                Err(e) => {
                    println!("Failed to load pdf: {:?}", e);
                    error_count += 1;
                }
            }
        }
    } //Generate jpg files

    if error_count > 0 {
        return Err(anyhow::anyhow!("Failed to load {} pdf files", error_count));
    }

    println!(
        "{} jpg Files generated",
        Path::new("./tmp2").read_dir()?.count()
    );
    Ok(())
}

async fn generate_webp_files() -> anyhow::Result<()> {
    let dir = Path::new("./tmp2");
    let mut count = 0;
    for file in dir.read_dir()? {
        let file = file?;
        let path = file.path();
        if path.is_file() && path.extension().and_then(|s| s.to_str()) == Some("jpg") {
            let img = image::open(&path)?.to_rgb8();
            let dynamic_img = image::DynamicImage::ImageRgb8(img);
            let encoder = Encoder::from_image(&dynamic_img).unwrap();
            let mut webp_data = encoder.encode(100.0);
            if webp_data.len() <= 50 * 1024 {
                let file_path = Path::new("./tmp3").join(format!(
                    "{}.webp",
                    path.file_stem().unwrap().to_str().unwrap()
                ));
                let mut file = File::create(file_path).await?;
                file.write_all(&webp_data).await?;
                return Ok(());
            }
            let quality = ((48.0 * 1024.0 / webp_data.len() as f32) * 100.0) as i32;
            webp_data = encoder.encode(std::cmp::max(quality, 5) as f32);

            let file_path = Path::new("./tmp3").join(format!(
                "{}.webp",
                path.file_stem().unwrap().to_str().unwrap()
            ));
            let mut file = File::create(file_path.clone()).await?;
            file.write_all(&webp_data).await?;
            count += 1;
        }
    } //Generate jpg files
    println!("{} webp Files generated", count);
    Ok(())
}

async fn upload_files(s3_client: &S3Client, bucket: String) -> anyhow::Result<()> {
    let dir = Path::new("./tmp2");
    println!("Uploading jpg files: {:#?}", dir.read_dir()?.count());
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
                key: path
                    .file_name()
                    .unwrap()
                    .to_string_lossy()
                    .as_ref()
                    .to_string(),
                body: Some(buf.into()),
                content_type: Some("image/jpeg".to_string()),
                ..Default::default()
            };
            s3_client.put_object(request).await?;
        }
    }
    let dir = Path::new("./tmp3");
    println!("Uploading webp files: {:#?}", dir.read_dir()?.count());
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
                key: path
                    .file_name()
                    .unwrap()
                    .to_string_lossy()
                    .as_ref()
                    .to_string(),
                body: Some(buf.into()),
                content_type: Some("image/webp".to_string()),
                ..Default::default()
            };
            s3_client.put_object(request).await?;
        }
    }
    //Upload files
    println!("Files uploaded");
    Ok(())
}

async fn get_temp_files(backend_url: &str, backend_token: &str) -> anyhow::Result<ApiResult> {
    let backend_client = reqwest::Client::new();
    let temp_files = backend_client
        .get(format!("{}/api/file/notPublished", backend_url))
        .bearer_auth(backend_token)
        .send()
        .await?
        .json::<ApiResult>()
        .await?;
    Ok(temp_files)
}

async fn get_publish_list(
    dir: &str,
    temp_files: &mut ApiResult,
) -> anyhow::Result<HashSet<String>> {
    let path = Path::new(dir);
    let mut local_files = HashSet::new();
    for file in path.read_dir()? {
        let file = file?;
        let path = file.path();
        if path.is_file() && path.extension().and_then(|s| s.to_str()) == Some("yml") {
            let file_name = path
                .file_name()
                .unwrap()
                .to_string_lossy()
                .as_ref()
                .to_string();
            local_files.insert(file_name[..32].to_string());
        }
    }

    let temp_filename = temp_files
        .files
        .iter()
        .map(|file| file.file_name[..32].to_string())
        .collect::<HashSet<_>>();
    let publish_list = local_files
        .intersection(&temp_filename)
        .cloned()
        .collect::<HashSet<_>>();
    temp_files
        .files
        .retain(|file| !publish_list.contains(&file.file_name[..32])); // Remove published files

    Ok(publish_list)
}

async fn publish_files(
    publish_list: HashSet<String>,
    backend_url: String,
    backend_token: String,
    temp_files: &mut ApiResult,
) -> anyhow::Result<()> {
    let mut ids = Vec::new();
    for file in &temp_files.files {
        if publish_list.contains(&file.file_name[..32]) {
            ids.push(file.id);
        }
    }
    println!("Publishing files: {:#?}", ids);
    let backend_client = reqwest::Client::new();
    let res = backend_client
        .post(format!("{}/api/file/publish", backend_url))
        .bearer_auth(backend_token)
        .json(&json!({
            "ids": ids
        }))
        .send()
        .await?
        .json::<serde_json::Value>()
        .await
        .unwrap();

    if true == res["success"].as_bool().unwrap() {
        println!("{} Files published", ids.len());
        Ok(())
    } else {
        println!(
            "Failed to publish files: {}",
            to_string_pretty(&res).unwrap()
        );
        Err(anyhow::anyhow!("Failed to publish files"))
    } //Publish files
}

async fn merge_json(dir: &str, s3_obj: &[Object]) -> anyhow::Result<()> {
    let dir = Path::new(dir);
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

            match metadata.data {
                Data::Book(ref mut book) => {
                    book.filesize = get_file_size(&metadata.id, s3_obj);
                    book.isbn = book
                        .isbn
                        .iter()
                        .map(|isbn| {
                            isbn.parse::<isbn::Isbn13>()
                                .unwrap()
                                .hyphenate()
                                .unwrap()
                                .to_string()
                        })
                        .collect();
                }
                Data::Doc(ref mut doc) => {
                    doc.filesize = get_file_size(&metadata.id, s3_obj);
                }
                Data::Test(ref mut test) => {
                    test.filesize = get_file_size(&metadata.id, s3_obj);
                }
            }

            json.push(metadata);
        }
    }
    let temp_file_path = dir.join("metadata2.json");
    let mut temp_file = File::create(&temp_file_path).await?;
    let json_data = serde_json::to_string(&json)?;
    temp_file.write_all(json_data.as_bytes()).await?;
    println!("Metadata JSON written to: {:?}", temp_file_path);
    //merge to json
    Ok(())
}

fn get_file_size(id: &str, s3_obj: &[Object]) -> Option<i64> {
    s3_obj
        .iter()
        .find(|file| {
            file.key == Some(format!("{}.pdf", id)) || file.key == Some(format!("{}.zip", id))
        })
        .and_then(|file| file.size)
}

async fn upload_metadata(
    r2_url: String,
    r2_access_key_id: String,
    r2_secret_access_key: String,
    r2_bucket: String,
    dir: &String,
) -> anyhow::Result<()> {
    let r2_client = get_s3_client(r2_url, r2_access_key_id, r2_secret_access_key).await?;
    let metadata_json = File::open(Path::new(&dir).join("metadata2.json")).await?;
    let mut reader = BufReader::new(metadata_json);
    let mut buf = Vec::new();
    reader.read_to_end(&mut buf).await?;
    let request = rusoto_s3::PutObjectRequest {
        bucket: r2_bucket.clone(),
        key: "metadata2.json".to_string(),
        body: Some(buf.into()),
        content_type: Some("application/json".to_string()),
        ..Default::default()
    };
    r2_client.put_object(request).await?;
    println!("Uploading metadata to R2");
    Ok(())
}

async fn list_all_objects(client: &rusoto_s3::S3Client, bucket_name: &str) -> Vec<Object> {
    let mut continuation_token: Option<String> = None;
    let mut s3_file_list = Vec::new();

    loop {
        let request = rusoto_s3::ListObjectsV2Request {
            bucket: bucket_name.to_string(),
            continuation_token: continuation_token.clone(),
            ..Default::default()
        };

        match client.list_objects_v2(request).await {
            Ok(output) => {
                if let Some(contents) = output.contents {
                    s3_file_list.extend(contents);
                }

                if let Some(next_continuation_token) = output.next_continuation_token {
                    continuation_token = Some(next_continuation_token);
                } else {
                    break;
                }
            }
            Err(rusoto_core::RusotoError::Unknown(resp)) => {
                eprintln!("Error: {}", resp.status);
                break;
            }
            Err(e) => {
                eprintln!("Error: {:?}", e);
                break;
            }
        }
    }
    s3_file_list
}
