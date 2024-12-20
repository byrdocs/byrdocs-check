use serde::Deserialize;

#[derive(serde::Deserialize, Debug)]
#[serde(deny_unknown_fields)]
pub enum Type {
    #[serde(rename = "test")]
    Test,
    #[serde(rename = "book")]
    Book,
    #[serde(rename = "doc")]
    Doc,
}

#[derive(serde::Deserialize, Debug)]
#[serde(deny_unknown_fields)]
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
#[serde(deny_unknown_fields)]
#[allow(dead_code)]
pub struct Test {
    pub college: Option<Vec<String>>,
    pub course: Course,
    pub time: Time,
    pub filetype: String,
    pub content: Vec<String>,
}

#[derive(serde::Deserialize, Debug)]
#[serde(deny_unknown_fields)]
#[allow(dead_code)]
pub struct Time {
    pub start: String,
    pub end: String,
    pub semester: Option<String>,
    pub stage: Option<String>,
}

#[derive(serde::Deserialize, Debug)]
#[serde(deny_unknown_fields)]
#[allow(dead_code)]
pub struct Course {
    #[serde(rename = "type")]
    pub type_: Option<String>,
    pub name: String,
}

#[derive(serde::Deserialize, Debug)]
#[serde(deny_unknown_fields)]
#[allow(dead_code)]
pub struct Book {
    pub title: String,
    pub authors: Vec<String>,
    pub translators: Option<Vec<String>>,
    pub edition: Option<String>,
    pub publish_year: Option<String>,
    pub publisher: Option<String>,
    pub isbn: Vec<String>,
    pub filetype: String,
}

#[derive(serde::Deserialize, Debug)]
#[serde(deny_unknown_fields)]
#[allow(dead_code)]
pub struct Doc {
    pub title: String,
    pub filetype: String,
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
