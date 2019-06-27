use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub enum Action {
    GET,
    SET,
    RM,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Command {
    pub action: Action,
    pub key: String,
    pub value: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub enum Response {
    Ok(Option<String>),
    Err(String),
}

impl Command {
    pub fn new(a: Action, k: String, v: String) -> Self {
        Command {
            action: a,
            key: k,
            value: v,
        }
    }
}
