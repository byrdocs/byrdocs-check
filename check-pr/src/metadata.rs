use serde::Deserialize;

#[derive(serde::Deserialize, Debug)]
pub enum Type {
    #[serde(rename = "test")]
    Test,
    #[serde(rename = "book")]
    Book,
    #[serde(rename = "doc")]
    Doc,
}

#[derive(serde::Deserialize, Debug)]
#[allow(dead_code)]
pub enum Data {
    Test(Test),
    Book(Book),
    Doc(Doc),
}

#[derive(Debug)]
#[allow(dead_code)]
pub struct MetaData {
    pub id: String,
    pub url: String,
    pub type_: Type,
    pub data: Data,
}

#[derive(serde::Deserialize, Debug)]
#[allow(dead_code)]
pub struct Test {
    pub title: String,
    pub college: Option<Vec<String>>,
    pub course: Course,
    pub time: Time,
    pub filetype: String,
    pub content: Vec<String>,
}

#[derive(serde::Deserialize, Debug)]
#[allow(dead_code)]
pub struct Time {
    start: String,
    end: String,
    pub semester: Option<String>,
    pub stage: Option<String>,
}

#[derive(serde::Deserialize, Debug)]
#[allow(dead_code)]
pub struct Course {
    #[serde(rename = "type")]
    pub type_: Option<String>,
    pub name: Option<String>,
}

#[derive(serde::Deserialize, Debug)]
#[allow(dead_code)]
pub struct Book {
    title: String,
    authors: Vec<String>,
    translators: Vec<String>,
    edition: Option<String>,
    publisher: String,
    isbn: Vec<String>,
    filetype: String,
}

#[derive(serde::Deserialize, Debug)]
#[allow(dead_code)]
pub struct Doc {
    title: String,
    filetype: String,
    pub course: Vec<Course>,
    pub content: Vec<String>,
}

impl<'de> Deserialize<'de> for MetaData {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let value = serde_yaml::Value::deserialize(deserializer)?;
        let data = match value.get("type").and_then(|t| t.as_str()) {
            Some("test") => Ok(Data::Test(
                serde_yaml::from_value(value.get("data").unwrap().clone())
                    .map_err(serde::de::Error::custom)?,
            )),
            Some("book") => Ok(Data::Book(
                serde_yaml::from_value(value.get("data").unwrap().clone())
                    .map_err(serde::de::Error::custom)?,
            )),
            Some("doc") => Ok(Data::Doc(
                serde_yaml::from_value(value.get("data").unwrap().clone())
                    .map_err(serde::de::Error::custom)?,
            )),
            _ => Err(serde::de::Error::custom("unknown type")),
        }?;
        Ok(MetaData {
            id: value["id"]
                .as_str()
                .ok_or_else(|| serde::de::Error::custom("missing id"))?
                .to_owned(),
            url: value["url"]
                .as_str()
                .ok_or_else(|| serde::de::Error::custom("missing url"))?
                .to_owned(),
            type_: serde_yaml::from_value(value["type"].clone())
                .map_err(serde::de::Error::custom)?,
            data,
        })
    }
}
