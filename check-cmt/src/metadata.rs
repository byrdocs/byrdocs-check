use serde::{ser::SerializeStruct, Deserialize, Serialize};

#[derive(serde::Deserialize, Debug, Serialize)]
pub enum Type {
    #[serde(rename = "test")]
    Test,
    #[serde(rename = "book")]
    Book,
    #[serde(rename = "doc")]
    Doc,
}

#[derive(serde::Deserialize, Debug, Serialize)]
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

#[derive(serde::Deserialize, Debug, Serialize)]
#[allow(dead_code)]
pub struct Test {
    pub title: String,
    pub college: Option<Vec<String>>,
    pub course: Course,
    pub time: Time,
    pub filetype: String,
    pub content: Vec<String>,
    pub filesize: Option<u64>,
}

#[derive(serde::Deserialize, Debug, Serialize)]
#[allow(dead_code)]
pub struct Time {
    start: String,
    end: String,
    pub semester: Option<String>,
    pub stage: Option<String>,
}

#[derive(serde::Deserialize, Debug, Serialize)]
#[allow(dead_code)]
pub struct Course {
    #[serde(rename = "type")]
    pub type_: Option<String>,
    pub name: Option<String>,
}

#[derive(serde::Deserialize, Debug, Serialize)]
#[allow(dead_code)]
pub struct Book {
    title: String,
    authors: Vec<String>,
    translators: Vec<String>,
    edition: Option<String>,
    publisher: String,
    publish_year: Option<String>,
    isbn: Vec<String>,
    filetype: String,
    pub filesize: Option<u64>,
}

#[derive(serde::Deserialize, Debug, Serialize)]
#[allow(dead_code)]
pub struct Doc {
    title: String,
    filetype: String,
    pub course: Vec<Course>,
    pub content: Vec<String>,
    pub filesize: Option<u64>,
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

impl Serialize for MetaData {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("MetaData", 4)?;
        state.serialize_field("id", &self.id)?;
        state.serialize_field("url", &self.url)?;
        state.serialize_field("type", &self.type_)?;
        match &self.data {
            Data::Test(test) => {
                state.serialize_field("data", &test)?;
            }
            Data::Book(book) => {
                state.serialize_field("data", &book)?;
            }
            Data::Doc(doc) => {
                state.serialize_field("data", &doc)?;
            }
        }
        state.end()
    }
}
