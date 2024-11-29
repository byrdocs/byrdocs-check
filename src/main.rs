mod metadata;

use regex;
use serde;
use serde_yaml;
use std::path::Path;
use structopt::StructOpt;

use crate::metadata::*;

#[derive(StructOpt)]
struct Input {
    #[structopt(help = "The path to check", required = true)]
    dir: String,
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

fn main() -> anyhow::Result<()> {
    let input = Input::from_args();

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
            let metadata: MetaData = match serde_yaml::from_reader(file) {
                Ok(metadata) => metadata,
                Err(e) => {
                    eprintln!("{}, {:?}", path.display(), e);
                    continue;
                }
            };

            if let Err(e) = check(&metadata.data, &metadata.id) {
                eprintln!("{}, {:?}", path.display(), e);
                continue;
            };

            let url_regex =
                regex::Regex::new(r"^https://byrdocs\.org/files/[a-fA-F0-9]{32}\.(pdf|zip)$")
                    .unwrap();

            if !url_regex.is_match(&metadata.url) {
                println!("请检查url是否填写正确: {}", path.display());
                continue;
            }

            if !metadata.url.contains(metadata.id.as_str()) {
                println!("请检查url中文件名与id是否匹配: {}", path.display());
                continue;
            }

            if !(path.file_name().unwrap().to_str().unwrap() == format!("{}.yml", metadata.id)) {
                println!("请检查文件名与id是否匹配: {}", path.display());
                continue;
            }

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
        return Err(anyhow::anyhow!("Some files are invalid"));
    }
    Ok(())
}

use std::sync::Mutex;
use std::sync::OnceLock;
static ISBNS: OnceLock<Mutex<Vec<(isbn::Isbn13, String)>>> = OnceLock::new();

fn check(data: &Data, md5: &str) -> anyhow::Result<()> {
    match data {
        Data::Test(test) => check_test(test)?,
        Data::Book(book) => check_book(book, md5)?,
        Data::Doc(doc) => check_doc(doc)?,
    }

    Ok(())
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
            errors.push(anyhow::anyhow!("重复的isbn md5: {} {}", md5, existing_md5));
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
        Err(anyhow::anyhow!(error_messages.join(", ")))
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
        Err(anyhow::anyhow!(error_messages.join(", ")))
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
        Err(anyhow::anyhow!(error_messages.join(", ")))
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
    }
}
