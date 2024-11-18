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

            match check_enum(&metadata.data) {
                Err(e) => {
                    eprintln!("{}, {:?}", path.display(), e);
                    continue;
                }
                _ => (),
            };

            let url_regex =
                regex::Regex::new(r"^https://byrdocs\.org/files/[a-fA-F0-9]{32}\.(pdf|zip)$")
                    .unwrap();

            if !url_regex.is_match(&metadata.url) | !metadata.url.contains(metadata.id.as_str()) {
                println!("请检查url与id是否匹配: {}", path.display());
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

fn check_enum(data: &Data) -> anyhow::Result<()> {
    match data {
        Data::Test(test) => {
            match &test.course.type_ {
                Some(test) => {
                    if !["本科", "研究生"].contains(&test.as_str()) {
                        return Err(anyhow::anyhow!(
                            "请检查course type，只能为\"本科\"或\"研究生\""
                        ));
                    }
                }
                None => (),
            }
            match &test.time.stage {
                Some(stage) => {
                    if !["期中", "期末"].contains(&stage.as_str()) {
                        return Err(anyhow::anyhow!("请检查stage，只能为\"期中\"或\"期末\""));
                    }
                }
                None => (),
            }
            match &test.time.semester {
                Some(semester) => {
                    if !["First", "Second"].contains(&semester.as_str()) {
                        return Err(anyhow::anyhow!(
                            "请检查semester，只能为\"First\"或\"Second\""
                        ));
                    }
                }
                None => (),
            }
            for content in &test.content {
                match content.as_str() {
                    "原题" => (),
                    "答案" => (),
                    _ => {
                        return Err(anyhow::anyhow!(
                            "错误的content，content只能为\"原题\"或\"答案\""
                        ))
                    }
                }
            }
            match &test.college {
                Some(colleges) => {
                    if colleges.contains(&"".to_string()) {
                        return Err(anyhow::anyhow!("college不能存在空字符串"));
                    }
                }
                _ => (),
            }
        }
        Data::Book(_) => (),
        Data::Doc(doc) => {
            for course in &doc.course {
                match &course.type_ {
                    Some(test) => {
                        if !["本科", "研究生"].contains(&test.as_str()) {
                            return Err(anyhow::anyhow!(
                                "请检查course type，只能为\"本科\"或\"研究生\""
                            ));
                        }
                    }
                    None => (),
                }
            }
            for content in &doc.content {
                match content.as_str() {
                    "思维导图" => (),
                    "题库" => (),
                    "答案" => (),
                    "知识点" => (),
                    "课件" => (),
                    _ => {
                        return Err(anyhow::anyhow!(
                            r#"错误的content，content只能为"思维导图"、"题库"、"答案"、"知识点"或"课件""#
                        ))
                    }
                }
            }
        }
    }

    Ok(())
}