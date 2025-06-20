use pdfium_render::prelude::{PdfRenderConfig, Pdfium};
use rusoto_core::HttpClient;
use rusoto_s3::{Object, S3, S3Client};
use serde::Serialize;
use serde_json::{json, to_string_pretty};
use std::{collections::HashSet, path::Path};
use tokio::fs::File;
use tokio::{
    self,
    io::{AsyncReadExt, AsyncWriteExt, BufReader},
};
use webp::Encoder;
use zip::{HasZipMetadata, ZipArchive};

use byrdocs_check::metadata::*;

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

struct Input {
    dir: String,
    s3_url: String,
    backend_url: String,
    backend_token: String,
    assess_key_id: String,
    secret_access_key: String,
    bucket: String,
    r2_url: String,
    r2_access_key_id: String,
    r2_secret_access_key: String,
    r2_bucket: String,
    filelist_url: String,
}

impl Input {
    fn new() -> Self {
        Self {
            dir: std::env::var("DIR").expect("DIR environment variable not set"),
            s3_url: std::env::var("S3_URL").expect("S3_URL environment variable not set"),
            backend_url: std::env::var("BACKEND_URL")
                .expect("BACKEND_URL environment variable not set"),
            backend_token: std::env::var("BACKEND_TOKEN")
                .expect("BACKEND_TOKEN environment variable not set"),
            assess_key_id: std::env::var("ACCESS_KEY_ID")
                .expect("ACCESS_KEY_ID environment variable not set"),
            secret_access_key: std::env::var("SECRET_ACCESS_KEY")
                .expect("SECRET_ACCESS_KEY environment variable not set"),
            bucket: std::env::var("BUCKET").expect("BUCKET environment variable not set"),
            r2_url: std::env::var("R2_URL").expect("R2_URL environment variable not set"),
            r2_access_key_id: std::env::var("R2_ACCESS_KEY_ID")
                .expect("R2_ACCESS_KEY_ID environment variable not set"),
            r2_secret_access_key: std::env::var("R2_SECRET_ACCESS_KEY")
                .expect("R2_SECRET_ACCESS_KEY environment variable not set"),
            r2_bucket: std::env::var("R2_BUCKET").expect("R2_BUCKET environment variable not set"),
            filelist_url: std::env::var("FILELIST_URL")
                .expect("FILELIST_URL environment variable not set"),
        }
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let input = Input::new();

    if !Path::new(&input.dir).exists() {
        eprintln!("The metadata directory does not exist: {}", input.dir);
        std::process::exit(1);
    }

    let s3_client =
        get_s3_client(input.s3_url, input.assess_key_id, input.secret_access_key).await?;
    let s3_obj = list_all_objects(&s3_client, &input.bucket).await;
    let mut api_result = get_temp_files(&input.backend_url, &input.backend_token).await?;

    let ids = get_publish_ids(&input.dir, &mut api_result).await?; // get publish list and remove published files from temp_files

    //image part

    let nocover_files = get_nocover_files(&s3_obj, &api_result).await?;

    std::fs::create_dir_all("./tmp1")?;
    std::fs::create_dir_all("./tmp2")?;
    std::fs::create_dir_all("./tmp3")?;
    download_files(&s3_client, &s3_obj, nocover_files, input.bucket.clone()).await?;

    generate_pdf_covers().await?;

    generate_zip_preview(&input.filelist_url).await?;

    reduce_webp_size().await?;

    upload_files(&s3_client, input.bucket).await?;

    //image part over

    publish_files(ids, input.backend_url, input.backend_token).await?; // publish files

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

async fn get_nocover_files(
    s3_obj: &Vec<Object>,
    temp_files: &ApiResult,
) -> anyhow::Result<HashSet<String>> {
    println!("Getting files need to generate cover");
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
        if !temp_files.files.iter().any(|file| file.file_name == key) {
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
    let files_need_generate_cover = jpg_diff.union(&webp_diff).cloned().collect::<HashSet<_>>();
    //Get files need to generate cover
    println!(
        "Files need to generate cover: {:#?}",
        files_need_generate_cover
    );
    Ok(files_need_generate_cover)
}

async fn download_files(
    s3_client: &S3Client,
    s3_obj: &Vec<Object>,
    files_need_update: HashSet<String>,
    bucket: String,
) -> anyhow::Result<()> {
    println!("Downloading files");
    std::fs::create_dir_all("./tmp1")?;
    let dir = Path::new("./tmp1");
    for file in s3_obj {
        let key = file.key.clone().unwrap();
        if files_need_update.contains(&key[..32]) {
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

async fn generate_pdf_covers() -> anyhow::Result<()> {
    println!("Generating images");
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
            } // Additional check for md5 mismatch
            match pdfium.load_pdf_from_file(path.to_str().unwrap(), None) {
                Ok(document) => {
                    let render_config = PdfRenderConfig::new()
                        .set_target_width(2000)
                        .set_maximum_height(2000);
                    let document_image = document
                        .pages()
                        .get(0)
                        .unwrap()
                        .render_with_config(&render_config)?
                        .as_image();
                    document_image.to_rgb8().save_with_format(
                        format!(
                            "./tmp2/{}.jpg",
                            &path.file_name().unwrap().to_str().unwrap()[..32]
                        ),
                        image::ImageFormat::Jpeg,
                    )?;
                    document_image.save_with_format(
                        format!(
                            "./tmp3/{}.webp",
                            &path.file_name().unwrap().to_str().unwrap()[..32]
                        ),
                        image::ImageFormat::WebP,
                    )?;
                }
                Err(e) => {
                    println!("Failed to load pdf: {:?}", e);
                    error_count += 1;
                }
            }
        }
    } //Generate images

    if error_count > 0 {
        return Err(anyhow::anyhow!("Failed to load {} pdf files", error_count));
    }

    println!(
        "{} jpg Files generated",
        Path::new("./tmp2").read_dir()?.count()
    );
    println!(
        "{} webp Files generated",
        Path::new("./tmp3").read_dir()?.count()
    );

    Ok(())
}

#[derive(Debug, Serialize)]
struct FileNode {
    #[serde(rename = "type")]
    node_type: String,
    name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    children: Option<Vec<FileNode>>,
}

fn build_tree(zip_path: &str) -> Result<Vec<FileNode>, anyhow::Error> {
    let file = std::fs::File::open(zip_path)?;
    let mut zip = ZipArchive::new(file)?;
    let mut root_children: Vec<FileNode> = Vec::new();

    for i in 0..zip.len() {
        let entry = zip.by_index(i)?;
        let is_dir = entry.is_dir();

        let raw_name = entry.name_raw();

        let name = if entry.get_metadata().is_utf8 || String::from_utf8(raw_name.to_vec()).is_ok() {
            String::from_utf8_lossy(raw_name).into_owned()
        } else {
            let (cow, _, _) = encoding_rs::GB18030.decode(raw_name);
            cow.into_owned()
        };

        let path = name.trim_matches('/').to_string();

        let parts: Vec<&str> = path.split('/').filter(|s| !s.is_empty()).collect();
        if parts.is_empty() {
            continue;
        }
        if parts[0] == "__MACOSX" {
            continue;
        }
        if parts
            .iter()
            .any(|part| part.starts_with('.') || part.starts_with("~$"))
        {
            continue;
        }

        let mut current_children = &mut root_children;
        for (index, part) in parts.iter().enumerate() {
            let is_last = index == parts.len() - 1;
            let node_type = if is_last && !is_dir { "file" } else { "folder" };

            let pos = current_children.iter().position(|n| n.name == *part);
            if let Some(pos) = pos {
                let node = &mut current_children[pos];
                if node.node_type != node_type {
                    return Err(anyhow::anyhow!(
                        "Conflict: {} is both {} and {}",
                        part,
                        node.node_type,
                        node_type
                    ));
                }
                if node_type == "folder" {
                    current_children = node.children.as_mut().unwrap();
                } else {
                    break;
                }
            } else {
                let new_node = FileNode {
                    node_type: node_type.to_string(),
                    name: part.to_string(),
                    children: if node_type == "folder" {
                        Some(Vec::new())
                    } else {
                        None
                    },
                };
                current_children.push(new_node);
                if node_type == "folder" {
                    let last_index = current_children.len() - 1;
                    current_children = current_children[last_index].children.as_mut().unwrap();
                } else {
                    break;
                }
            }
        }
    }

    Ok(root_children)
}

async fn generate_zip_preview(filelist_url: &str) -> anyhow::Result<()> {
    println!("Generating zip preview");
    let dir = Path::new("./tmp1");
    for file in dir.read_dir()? {
        let file = file?;
        let path = file.path();
        if path.is_file() && path.extension().and_then(|s| s.to_str()) == Some("zip") {
            println!("Processing zip file: {:?}", path.to_string_lossy().as_ref());
            let tree = build_tree(path.to_str().unwrap())?;
            let res = reqwest::Client::new()
                .post(filelist_url)
                .header("Content-Type", "application/json")
                .json(&json!({
                    "height": 425,
                    "width": 300,
                    "fontSize": 14,
                    "files": tree
                }))
                .send()
                .await;

            let res = match res {
                Ok(res) => res,
                Err(e) => {
                    println!(
                        "Generate zip preview failed: {}; file: {}",
                        e,
                        file.file_name().into_string().unwrap()
                    );
                    continue;
                }
            };

            let bytes = res.bytes().await?;

            let img = image::load_from_memory(&bytes)?;
            img.to_rgb8().save_with_format(
                format!("./tmp2/{}.jpg", path.file_stem().unwrap().to_str().unwrap()),
                image::ImageFormat::Jpeg,
            )?;
            img.save_with_format(
                format!(
                    "./tmp3/{}.webp",
                    path.file_stem().unwrap().to_str().unwrap()
                ),
                image::ImageFormat::WebP,
            )?;
            println!(
                "Preview generated for zip file: {:?}",
                path.to_string_lossy().as_ref()
            );
        }
    } //Generate zip preview

    println!("Zip preview generated");
    Ok(())
}

async fn reduce_webp_size() -> anyhow::Result<()> {
    println!("Reducing webp file size");
    let dir = Path::new("./tmp3");
    let mut count = 0;
    for file in dir.read_dir()? {
        let file = file?;
        let path = file.path();
        if path.is_file() && path.extension().and_then(|s| s.to_str()) == Some("webp") {
            let img = image::open(&path)?;
            let encoder = Encoder::from_image(&img).unwrap();
            let mut webp_data = encoder.encode(100.0);
            if webp_data.len() > 50 * 1024 {
                let quality = ((48.0 * 1024.0 / webp_data.len() as f32) * 100.0) as i32;
                webp_data = encoder.encode(std::cmp::max(quality, 5) as f32)
            };
            let file_path = Path::new("./tmp3").join(format!(
                "{}.webp",
                path.file_stem().unwrap().to_str().unwrap()
            ));
            let mut file = File::create(file_path.clone()).await?;
            file.write_all(&webp_data).await?;
            count += 1;
        }
    } //Reduce webp size
    println!("{} webp file size reduced", count);
    Ok(())
}

async fn upload_files(s3_client: &S3Client, bucket: String) -> anyhow::Result<()> {
    let jpg_dir = Path::new("./tmp2");
    println!("Uploading jpg files: {:#?}", jpg_dir.read_dir()?.count());
    let mut jpg_success = 0;
    for file in jpg_dir.read_dir()? {
        let file = file?;
        let path = file.path();
        if path.is_file() && path.extension().and_then(|s| s.to_str()) == Some("jpg") {
            let file = File::open(&path).await?;
            let mut reader = BufReader::new(file);
            let mut buf = Vec::new();
            reader.read_to_end(&mut buf).await?;
            let file_name = path
                .file_name()
                .unwrap()
                .to_string_lossy()
                .as_ref()
                .to_string();
            let mut retry = 3;
            while let Err(e) = s3_client
                .put_object(rusoto_s3::PutObjectRequest {
                    bucket: bucket.clone(),
                    key: file_name.clone(),
                    body: Some(buf.clone().into()),
                    content_type: Some("image/jpeg".to_string()),
                    ..Default::default()
                })
                .await
            {
                retry -= 1;
                if retry <= 0 {
                    break;
                }
                println!("{file_name}: 上传失败: {e}，剩余重试次数: {retry}")
            }
            if retry <= 0 {
                println!("{file_name}: 上传失败！跳过此文件")
            } else {
                println!("{file_name}: 上传成功!");
                jpg_success += 1;
            }
        }
    }
    let jpg_failed = jpg_dir.read_dir()?.count() - jpg_success;
    println!("Jpg files uploaded, success: {jpg_success}, failed: {jpg_failed}",);
    let webp_dir = Path::new("./tmp3");
    println!("Uploading webp files: {:#?}", webp_dir.read_dir()?.count());
    let mut webp_success = 0;
    for file in webp_dir.read_dir()? {
        let file = file?;
        let path = file.path();
        if path.is_file() && path.extension().and_then(|s| s.to_str()) == Some("webp") {
            let file = File::open(&path).await?;
            let mut reader = BufReader::new(file);
            let mut buf = Vec::new();
            reader.read_to_end(&mut buf).await?;
            let file_name = path
                .file_name()
                .unwrap()
                .to_string_lossy()
                .as_ref()
                .to_string();
            let mut retry = 3;
            while let Err(e) = s3_client
                .put_object(rusoto_s3::PutObjectRequest {
                    bucket: bucket.clone(),
                    key: path
                        .file_name()
                        .unwrap()
                        .to_string_lossy()
                        .as_ref()
                        .to_string(),
                    body: Some(buf.clone().into()),
                    content_type: Some("image/webp".to_string()),
                    ..Default::default()
                })
                .await
            {
                retry -= 1;
                if retry <= 0 {
                    break;
                }
                println!("{file_name}: 上传失败: {e}，剩余重试次数: {retry}")
            }
            if retry <= 0 {
                println!("{file_name}: 上传失败！跳过此文件")
            } else {
                println!("{file_name}: 上传成功!");
                webp_success += 1;
            }
        }
    }
    let webp_failed = webp_dir.read_dir()?.count() - webp_success;
    println!("WebP files uploaded, success: {webp_success}, failed: {webp_failed}");
    println!("Files uploaded");
    if !jpg_failed == 0 || !webp_failed == 0 {
        Err(anyhow::anyhow!("Upload files failed"))
    } else {
        Ok(())
    }
    //Upload files
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

async fn get_publish_ids(dir: &str, api_result: &mut ApiResult) -> anyhow::Result<HashSet<u64>> {
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

    let temp_filename = api_result
        .files
        .iter()
        .map(|file| file.file_name[..32].to_string())
        .collect::<HashSet<_>>();
    let publish_list = local_files
        .intersection(&temp_filename)
        .cloned()
        .collect::<HashSet<_>>();
    let ids = api_result
        .files
        .iter()
        .filter(|file| publish_list.contains(&file.file_name[..32]))
        .map(|file| file.id)
        .collect::<HashSet<_>>();
    api_result
        .files
        .retain(|file| !publish_list.contains(&file.file_name[..32]));

    Ok(ids)
}

async fn publish_files(
    ids: HashSet<u64>,
    backend_url: String,
    backend_token: String,
) -> anyhow::Result<()> {
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
    println!("Uploading metadata to R2");
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
    println!("Metadata uploaded");
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

#[cfg(test)]
mod test {
    use crate::generate_zip_preview;

    #[tokio::test]
    async fn test_zip_preview() {
        println!("Current directory: {:?}", std::env::current_dir().unwrap());
        generate_zip_preview("https://filelist.byrdocs.org/png")
            .await
            .unwrap();
    }
}
