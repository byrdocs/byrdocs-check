mod metadata;

use rusoto_core::HttpClient;
use rusoto_s3::S3;
use std::path::Path;
use structopt::StructOpt;
use tokio::{
    self,
    io::{AsyncReadExt, BufReader},
};

use crate::metadata::*;

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
    let s3_file_list = list_all_objects(&s3_client, &input.bucket).await;

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
        let file_name = entry.file_name();
        if !file_name.to_str().unwrap().ends_with(".yml") {
            if file_name.to_str().unwrap().ends_with(".yaml") {
                println!("{:?}:\n  请将.yml改为.yaml", file_name);
            } else {
                println!("{:?}:\n  请检查文件名后缀, 只能为\".yml\"", file_name);
            }
            continue;
        }
        let path = entry.path();
        if path.is_file() {
            let file = std::fs::File::open(&path)?;
            let metadata: MetaData = match serde_yaml::from_reader(file) {
                Ok(metadata) => metadata,
                Err(e) => {
                    println!("{:?}:\n  格式错误: {:?}", path.file_name().unwrap(), e);
                    continue;
                }
            };

            if let Err(e) = check(
                &metadata,
                &metadata.id,
                &s3_file_list,
                &path,
                &temp_files,
                &input.bucket,
                &s3_client,
            )
            .await
            {
                println!("{:?}:\n  {:?}", path.file_name().unwrap(), e);
                continue;
            };

            match metadata.data {
                Data::Test(_) => {
                    success_test += 1;
                }
                Data::Book(_) => {
                    success_book += 1;
                }
                Data::Doc(_) => {
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
        return Err(anyhow::anyhow!(
            "自动检查不通过，请按照上述报错信息修改您的元信息"
        ));
    } else {
        println!("自动检查通过，我们正在审核您的文件，请耐心等待");
    }
    Ok(())
}

async fn list_all_objects(client: &rusoto_s3::S3Client, bucket_name: &str) -> Vec<String> {
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
                    s3_file_list.extend(
                        contents
                            .iter()
                            .map(|item| item.key.clone().unwrap())
                            .collect::<Vec<_>>(),
                    )
                }

                if let Some(next_continuation_token) = output.next_continuation_token {
                    continuation_token = Some(next_continuation_token);
                } else {
                    break;
                }
            }
            Err(rusoto_core::RusotoError::Unknown(resp)) => {
                println!("Error: {}", resp.status);
                break;
            }
            Err(e) => {
                println!("Error: {:?}", e);
                break;
            }
        }
    }
    s3_file_list
}

use std::sync::Mutex;
use std::sync::OnceLock;
static ISBNS: OnceLock<Mutex<Vec<(isbn::Isbn13, String)>>> = OnceLock::new();

async fn check(
    metadata: &MetaData,
    md5: &str,
    s3_file_list: &Vec<String>,
    path: &Path,
    temp_files: &ApiResult,
    bucket: &str,
    s3_client: &rusoto_s3::S3Client,
) -> anyhow::Result<()> {
    let mut errors = Vec::new();

    let data = &metadata.data;
    if let Err(e) = match data {
        Data::Test(test) => check_test(test),
        Data::Book(book) => check_book(book, md5),
        Data::Doc(doc) => check_doc(doc),
    } {
        errors.push(e);
    }

    let url_regex =
        regex::Regex::new(r"^https://byrdocs\.org/files/[a-fA-F0-9]{32}\.(pdf|zip)$").unwrap();

    if !s3_file_list.contains(&metadata.url[metadata.url.len() - 36..].to_string()) {
        errors.push(anyhow::anyhow!("请检查文件是否上传"));
    }

    if !url_regex.is_match(&metadata.url) {
        errors.push(anyhow::anyhow!("请检查url是否填写正确"));
    }

    if !metadata.url.contains(metadata.id.as_str()) {
        errors.push(anyhow::anyhow!("请检查url中文件名与id是否匹配"));
    }

    if !(path.file_name().unwrap().to_str().unwrap() == format!("{}.yml", metadata.id)) {
        errors.push(anyhow::anyhow!("请检查文件名是否与id匹配"));
    }

    let mut unmatched = false;
    for temp_file in &temp_files.files {
        if temp_file.file_name.as_str() == format!("{}.pdf", metadata.id)
            || temp_file.file_name.as_str() == format!("{}.zip", metadata.id)
        {
            match temp_file.status {
                Status::Published => {
                    // println!("Published: {}", path.display());
                }
                _ => {
                    let request = rusoto_s3::GetObjectRequest {
                        bucket: bucket.to_string(),
                        key: metadata.url[metadata.url.len() - 36..].to_string(),
                        ..Default::default()
                    };
                    let file = s3_client.get_object(request).await?.body.unwrap();
                    let mut reader = BufReader::new(file.into_async_read());
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
                    if !(format!("{:x}", md5) == metadata.id) {
                        unmatched = true;
                    }
                }
            }
            break;
        }
    }
    if unmatched {
        errors.push(anyhow::anyhow!("md5不匹配"));
    }

    if errors.is_empty() {
        Ok(())
    } else {
        let error_messages: Vec<String> = errors.iter().map(|e| e.to_string()).collect();
        Err(anyhow::anyhow!(error_messages.join("\n")))
    }
}

fn check_book(book: &Book, md5: &str) -> anyhow::Result<()> {
    let mut errors = Vec::new();
    if book.authors.len() == 0 {
        errors.push(anyhow::anyhow!("应当至少有一个作者"));
    }
    let year_regex = regex::Regex::new(r"^\d{4}$").unwrap();
    if let Some(year) = &book.publish_year {
        if !year_regex.is_match(year) {
            errors.push(anyhow::anyhow!("请检查出版年份"));
        }
    }
    book.isbn.iter().for_each(|isbn| {
        if let Err(e) = isbn.parse::<isbn::Isbn13>() {
            errors.push(anyhow::anyhow!("请检查isbn格式: {}", e));
        }
    });
    for isbn in book.isbn.clone() {
        let isbn = isbn.parse::<isbn::Isbn13>().unwrap();

        let mut isbns_lock = ISBNS.get_or_init(|| Mutex::new(Vec::new())).lock().unwrap();
        if let Some((_, existing_md5)) = isbns_lock.iter().find(|(i, _)| i == &isbn) {
            errors.push(anyhow::anyhow!("重复的isbn. md5: {} {}", md5, existing_md5));
        } else {
            isbns_lock.push((isbn, md5.to_string()));
        }
    }
    if book.filetype != "pdf" {
        errors.push(anyhow::anyhow!("请检查filetype，只能为pdf"));
    }
    if errors.is_empty() {
        Ok(())
    } else {
        let error_messages: Vec<String> = errors.iter().map(|e| e.to_string()).collect();
        Err(anyhow::anyhow!(error_messages.join("\n")))
    }
}

fn check_test(test: &Test) -> anyhow::Result<()> {
    let mut errors = Vec::new();
    if let Some(test) = &test.course.type_ {
        if !["本科", "研究生"].contains(&test.as_str()) {
            errors.push(anyhow::anyhow!(
                "请检查course type，只能为\"本科\"或\"研究生\""
            ));
        }
    }
    if let Some(stage) = &test.time.stage {
        if !["期中", "期末"].contains(&stage.as_str()) {
            errors.push(anyhow::anyhow!("请检查stage，只能为\"期中\"或\"期末\""));
        }
    }
    if let Some(semester) = &test.time.semester {
        if !["First", "Second"].contains(&semester.as_str()) {
            errors.push(anyhow::anyhow!(
                "请检查semester，只能为\"First\"或\"Second\""
            ));
        }
    }
    for content in &test.content {
        match content.as_str() {
            "原题" => (),
            "答案" => (),
            _ => errors.push(anyhow::anyhow!(
                "错误的content，content只能为\"原题\"或\"答案\""
            )),
        }
    }
    if let Some(colleges) = &test.college {
        if colleges.contains(&"".to_string()) {
            errors.push(anyhow::anyhow!("college不能存在空字符串"));
        }
    }
    if let (Ok(start), Ok(end)) = (test.time.start.parse::<u32>(), test.time.end.parse::<u32>()) {
        if !(start == end || start + 1 == end) {
            errors.push(anyhow::anyhow!("请检查时间"));
        }
    } else {
        errors.push(anyhow::anyhow!("时间格式不正确"));
    }
    if test.filetype != "pdf" && test.filetype != "zip" {
        errors.push(anyhow::anyhow!("请检查filetype，只能为pdf或zip"));
    }
    if test.content.len() == 0 {
        errors.push(anyhow::anyhow!("content不能为空"));
    }
    if errors.is_empty() {
        Ok(())
    } else {
        let error_messages: Vec<String> = errors.iter().map(|e| e.to_string()).collect();
        Err(anyhow::anyhow!(error_messages.join("\n")))
    }
}

fn check_doc(doc: &Doc) -> anyhow::Result<()> {
    let mut errors = Vec::new();
    if &doc.course.len() == &0 {
        errors.push(anyhow::anyhow!("course不能为空"));
    }
    for course in &doc.course {
        if let Some(test) = &course.type_ {
            if !["本科", "研究生"].contains(&test.as_str()) {
                errors.push(anyhow::anyhow!(
                    "请检查course type，只能为\"本科\"或\"研究生\""
                ));
            }
        }
    }
    if &doc.content.len() == &0 {
        errors.push(anyhow::anyhow!("content不能为空"));
    }
    for content in &doc.content {
        match content.as_str() {
            "思维导图" => (),
            "题库" => (),
            "答案" => (),
            "知识点" => (),
            "课件" => (),
            _ => errors.push(anyhow::anyhow!(
                r#"错误的content，content只能为"思维导图"、"题库"、"答案"、"知识点"或"课件""#
            )),
        }
    }
    if doc.filetype != "pdf" && doc.filetype != "zip" {
        errors.push(anyhow::anyhow!("请检查filetype，只能为pdf或zip"));
    }
    if errors.is_empty() {
        Ok(())
    } else {
        let error_messages: Vec<String> = errors.iter().map(|e| e.to_string()).collect();
        Err(anyhow::anyhow!(error_messages.join("\n")))
    }
}

#[cfg(test)]
mod test {
    #[test]
    fn test_isbn() {
        let isbn = "978-7-111-40772-1";
        assert_eq!(
            isbn.parse::<isbn::Isbn13>(),
            Err(isbn::IsbnError::InvalidDigit)
        );
        let isbn = "978-7-111-40772-0";
        assert_eq!(
            isbn.parse::<isbn::Isbn13>(),
            Ok(isbn::Isbn13::new([9, 7, 8, 7, 1, 1, 1, 4, 0, 7, 7, 2, 0]).unwrap())
        );
        let isbn = "9787111407720";
        assert_eq!(
            isbn.parse::<isbn::Isbn13>(),
            Ok(isbn::Isbn13::new([9, 7, 8, 7, 1, 1, 1, 4, 0, 7, 7, 2, 0]).unwrap())
        );
        assert_eq!(
            "978-7-111-40772-0",
            isbn.parse::<isbn::Isbn13>()
                .unwrap()
                .hyphenate()
                .unwrap()
                .as_str()
        );
    }
}
