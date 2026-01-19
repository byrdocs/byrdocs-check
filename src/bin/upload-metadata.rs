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

use byrdocs_check::{get_env,metadata::*};

#[derive(serde::Deserialize, Debug)]
#[allow(dead_code)]
struct ApiResult {
    success: bool,
    files: Vec<TempFiles>,
}

#[derive(serde::Deserialize, Debug, Eq, PartialEq, Hash, Clone)]
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

#[derive(serde::Deserialize, Debug, Eq, PartialEq, Hash, Clone)]
enum Status {
    Published,
    Pending,
    Timeout,
    Expired,
    Error,
    Uploaded,
}

struct Input {
    metadata_dir: String,
    r2_endpoint: String,
    r2_access_key_id: String,
    r2_secret_access_key: String,
    r2_file_bucket: String,
    r2_data_bucket: String,
    byrdocs_site_url: String,
    byrdocs_site_token: String,
    filelist_site_url: String,
    backup_endpoint: String,
    backup_access_key_id: String,
    backup_secret_access_key: String,
    backup_file_bucket: String,
}

impl Input {
    fn new() -> Self {
        Self {
            metadata_dir: get_env("METADATA_DIR"),
            r2_endpoint: format!(
                "https://{}.r2.cloudflarestorage.com",
                get_env("R2_ACCOUNT_ID"),
            ),
            r2_access_key_id: get_env("R2_ACCESS_KEY_ID"),
            r2_secret_access_key: get_env("R2_SECRET_ACCESS_KEY"),
            r2_file_bucket: get_env("R2_FILE_BUCKET"),
            r2_data_bucket: get_env("R2_DATA_BUCKET"),
            byrdocs_site_url: get_env("BYRDOCS_SITE_URL"),
            byrdocs_site_token: get_env("BYRDOCS_SITE_TOKEN"),
            filelist_site_url: get_env("FILELIST_SITE_URL"),
            backup_endpoint: get_env("BACKUP_ENDPOINT"),
            backup_access_key_id: get_env("BACKUP_ACCESS_KEY_ID"),
            backup_secret_access_key: get_env("BACKUP_SECRET_ACCESS_KEY"),
            backup_file_bucket: get_env("BACKUP_FILE_BUCKET"),
        }
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let input = Input::new();

    if !Path::new(&input.metadata_dir).exists() {
        eprintln!("The metadata directory does not exist: {}", input.metadata_dir);
        std::process::exit(1);
    }

    let s3_client = get_s3_client(
        input.r2_endpoint.clone(),
        input.r2_access_key_id.clone(),
        input.r2_secret_access_key.clone(),
    )
    .await?;
    let s3_obj = list_all_objects(&s3_client, &input.r2_file_bucket).await;
    let backup_client = get_s3_client(
        input.backup_endpoint.clone(),
        input.backup_access_key_id.clone(),
        input.backup_secret_access_key.clone(),
    )
    .await?;
    let mut api_result = get_temp_files(&input.byrdocs_site_url, &input.byrdocs_site_token).await?;

    let need_publish_files = get_publish_files(&input.metadata_dir, &mut api_result).await?; // get publish list and remove published files from temp_files

    //image part

    let nocover_files = get_nocover_files(&s3_obj, &api_result).await?;

    std::fs::create_dir_all("./tmp1")?;
    std::fs::create_dir_all("./tmp2")?;
    std::fs::create_dir_all("./tmp3")?;
    download_files(
        &s3_client,
        &s3_obj,
        need_publish_files
            .iter()
            .map(|f| f.file_name[..32].to_string())
            .collect::<HashSet<_>>(),
        input.r2_file_bucket.clone(),
    )
    .await?;

    generate_pdf_covers(nocover_files.clone()).await?;

    generate_zip_preview(&input.filelist_site_url, nocover_files.clone()).await?;

    reduce_webp_size().await?;

    upload_images(&s3_client, input.r2_file_bucket).await?;

    //image part over

    publish_files(need_publish_files, input.byrdocs_site_url, input.byrdocs_site_token).await?; // publish files

    // Upload to backup storage
    backup_files(&backup_client, input.backup_file_bucket).await?;

    merge_json(&input.metadata_dir, &s3_obj).await?;

    upload_metadata(
        input.r2_endpoint,
        input.r2_access_key_id,
        input.r2_secret_access_key,
        input.r2_data_bucket,
        &input.metadata_dir,
    )
    .await?;

    println!("All done");

    Ok(())
}

async fn get_s3_client(
    r2_endpoint: String,
    access_key_id: String,
    secret_access_key: String,
) -> anyhow::Result<S3Client> {
    let http_client = HttpClient::new()?;
    let credentials = rusoto_core::credential::StaticProvider::new_minimal(access_key_id, secret_access_key);
    let region = rusoto_core::Region::Custom {
        name: "auto".to_owned(),
        endpoint: r2_endpoint,
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
        if files_need_update.contains(&key[..32])
            && (key.ends_with(".pdf") || key.ends_with(".zip"))
        {
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

async fn generate_pdf_covers(nocover_files: HashSet<String>) -> anyhow::Result<()> {
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
        if path.is_file()
            && path.extension().and_then(|s| s.to_str()) == Some("pdf")
            && nocover_files.contains(&path.file_name().unwrap().to_str().unwrap()[..32])
        {
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

async fn generate_zip_preview(
    filelist_site_url: &str,
    nocover_files: HashSet<String>,
) -> anyhow::Result<()> {
    println!("Generating zip preview");
    let dir = Path::new("./tmp1");
    for file in dir.read_dir()? {
        let file = file?;
        let path = file.path();
        if path.is_file()
            && path.extension().and_then(|s| s.to_str()) == Some("zip")
            && nocover_files.contains(&path.file_name().unwrap().to_str().unwrap()[..32])
        {
            println!("Processing zip file: {:?}", path.to_string_lossy().as_ref());
            let tree = build_tree(path.to_str().unwrap())?;
            let res = reqwest::Client::new()
                .post(filelist_site_url)
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

async fn upload_images(s3_client: &S3Client, bucket: String) -> anyhow::Result<()> {
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

async fn backup_files(s3_client: &S3Client, bucket: String) -> anyhow::Result<()> {
    let file_dir = Path::new("./tmp1");
    println!(
        "Uploading backup files: {:#?}",
        file_dir.read_dir()?.count()
    );
    let mut file_success = 0;
    for file in file_dir.read_dir()? {
        let file = file?;
        let path = file.path();
        if path.is_file() {
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

            'retry_loop: while retry > 0 {
                let create_result = match s3_client
                    .create_multipart_upload(rusoto_s3::CreateMultipartUploadRequest {
                        bucket: bucket.clone(),
                        key: file_name.clone(),
                        storage_class: Some("STANDARD".to_string()),
                        ..Default::default()
                    })
                    .await
                {
                    Ok(result) => result,
                    Err(e) => {
                        retry -= 1;
                        println!("{file_name}: 创建上传请求失败: {e}，剩余重试次数: {retry}");
                        continue 'retry_loop;
                    }
                };

                let upload_id = match create_result.upload_id {
                    Some(id) => id,
                    None => {
                        retry -= 1;
                        println!("{file_name}: 无法获取上传ID，剩余重试次数: {retry}");
                        continue 'retry_loop;
                    }
                };

                const CHUNK_SIZE: usize = 5 * 1024 * 1024;
                let mut parts = Vec::new();
                let mut upload_failed = false;

                for (part_number, chunk) in buf.chunks(CHUNK_SIZE).enumerate() {
                    let part_number = (part_number + 1) as i64;

                    match s3_client
                        .upload_part(rusoto_s3::UploadPartRequest {
                            bucket: bucket.clone(),
                            key: file_name.clone(),
                            upload_id: upload_id.clone(),
                            part_number,
                            body: Some(chunk.to_vec().into()),
                            ..Default::default()
                        })
                        .await
                    {
                        Ok(upload_result) => {
                            parts.push(rusoto_s3::CompletedPart {
                                e_tag: upload_result.e_tag,
                                part_number: Some(part_number),
                            });
                        }
                        Err(e) => {
                            println!("{file_name}: 分块 {part_number} 上传失败: {e}");
                            upload_failed = true;
                            break;
                        }
                    }
                }

                if upload_failed {
                    retry -= 1;
                    println!("{file_name}: 分块上传失败，剩余重试次数: {retry}");

                    let _ = s3_client
                        .abort_multipart_upload(rusoto_s3::AbortMultipartUploadRequest {
                            bucket: bucket.clone(),
                            key: file_name.clone(),
                            upload_id: upload_id.clone(),
                            ..Default::default()
                        })
                        .await;

                    continue 'retry_loop;
                }

                match s3_client
                    .complete_multipart_upload(rusoto_s3::CompleteMultipartUploadRequest {
                        bucket: bucket.clone(),
                        key: file_name.clone(),
                        upload_id,
                        multipart_upload: Some(rusoto_s3::CompletedMultipartUpload {
                            parts: Some(parts),
                        }),
                        ..Default::default()
                    })
                    .await
                {
                    Ok(_) => {
                        println!("{file_name}: 上传成功!");
                        file_success += 1;
                        break 'retry_loop;
                    }
                    Err(e) => {
                        retry -= 1;
                        println!("{file_name}: 完成上传失败: {e}，剩余重试次数: {retry}");
                    }
                }
            }

            if retry <= 0 {
                println!("{file_name}: 上传失败！跳过此文件");
            }
        }
    }
    let file_failed = file_dir.read_dir()?.count() - file_success;
    println!("Backup files uploaded, success: {file_success}, failed: {file_failed}");
    if file_failed != 0 {
        Err(anyhow::anyhow!("Upload backup files failed"))
    } else {
        Ok(())
    }
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

async fn get_publish_files(
    dir: &str,
    api_result: &mut ApiResult,
) -> anyhow::Result<HashSet<TempFiles>> {
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
        .cloned()
        .collect::<HashSet<_>>();
    api_result
        .files
        .retain(|file| !publish_list.contains(&file.file_name[..32]));

    Ok(ids)
}

async fn publish_files(
    files: HashSet<TempFiles>,
    backend_url: String,
    backend_token: String,
) -> anyhow::Result<()> {
    let ids = files.iter().map(|file| file.id).collect::<Vec<_>>();
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
    let temp_file_path = dir.join("metadata.json");
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
    bucket: String,
    dir: &String,
) -> anyhow::Result<()> {
    println!("Uploading metadata to R2");
    let r2_client = get_s3_client(r2_url, r2_access_key_id, r2_secret_access_key).await?;
    let metadata_json = File::open(Path::new(&dir).join("metadata.json")).await?;
    let mut reader = BufReader::new(metadata_json);
    let mut buf = Vec::new();
    reader.read_to_end(&mut buf).await?;
    let request = rusoto_s3::PutObjectRequest {
        bucket: bucket.clone(),
        key: "metadata.json".to_string(),
        body: Some(buf.into()),
        content_type: Some("application/json".to_string()),
        ..Default::default()
    };
    r2_client.put_object(request).await?;
    println!("Metadata uploaded");
    Ok(())
}

async fn list_all_objects(client: &rusoto_s3::S3Client, bucket: &str) -> Vec<Object> {
    let mut continuation_token: Option<String> = None;
    let mut s3_file_list = Vec::new();

    loop {
        let request = rusoto_s3::ListObjectsV2Request {
            bucket: bucket.to_string(),
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
    use std::collections::HashSet;

    use crate::generate_zip_preview;

    #[tokio::test]
    async fn test_zip_preview() {
        println!("Current directory: {:?}", std::env::current_dir().unwrap());
        generate_zip_preview("https://filelist.byrdocs.org/png", HashSet::new())
            .await
            .unwrap();
    }
}
