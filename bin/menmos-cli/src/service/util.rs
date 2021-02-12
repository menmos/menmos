use std::collections::HashMap;

use anyhow::{ensure, Result};

pub fn convert_meta_vec_to_map(meta_vec: Vec<String>) -> Result<HashMap<String, String>> {
    meta_vec
        .into_iter()
        .map(|v| {
            let split: Vec<_> = v.split(':').collect();
            ensure!(split.len() == 2, "bad meta map");
            Ok((split[0].to_string(), split[1].to_string()))
        })
        .collect()
}
