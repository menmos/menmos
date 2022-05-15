use anyhow::{bail, Result};
use std::str::FromStr;

use clap::Parser;

use menmos::Menmos;

use rood::cli::OutputManager;

enum SortBy {
    Name,
    Count,
}

impl Default for SortBy {
    fn default() -> Self {
        Self::Count
    }
}

impl FromStr for SortBy {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        match s.to_lowercase().as_ref() {
            "name" => Ok(Self::Name),
            "count" => Ok(Self::Count),
            _ => bail!("unknown value: '{s}'"),
        }
    }
}

#[derive(Parser)]
pub struct ListMetadataCommand {
    /// Whether to list tags.
    #[clap(long = "tags", short = 't')]
    tags: bool,

    /// The fields to list. Lists every field if none are specified.
    #[clap(long = "field", short = 'f')]
    fields: Vec<String>,

    /// Whether to expand the values of the fields.
    #[clap(long = "expand-values")]
    expand_values: bool,

    /// Whether to sort the output by name (default), or by count.
    #[clap(long = "sort-by", default_value = "name")]
    sort_by: SortBy,
}

impl ListMetadataCommand {
    pub async fn run(self, cli: OutputManager, client: Menmos) -> Result<()> {
        let field_filter = if self.fields.is_empty() {
            None
        } else {
            Some(self.fields.clone())
        };

        let pushed = cli.push();

        let resp = client.client().list_meta(None, field_filter).await?;

        if self.tags {
            cli.step("tags");

            let mut tag_vec = resp.tags.into_iter().collect::<Vec<_>>();
            tag_vec.sort_by(|(a_name, a_count), (b_name, b_count)| match self.sort_by {
                SortBy::Count => a_count.cmp(b_count),
                SortBy::Name => a_name.cmp(b_name),
            });

            for (tag, count) in tag_vec {
                pushed.step(format!("{tag} -> {count}",))
            }
        }

        let detail = pushed.push();
        cli.step("fields");
        for (field, values) in resp.fields {
            pushed.step(field);
            if self.expand_values || !self.fields.is_empty() {
                let mut field_vec = values.into_iter().collect::<Vec<_>>();
                field_vec.sort_by(|(a_name, a_count), (b_name, b_count)| match self.sort_by {
                    SortBy::Count => a_count.cmp(b_count),
                    SortBy::Name => a_name.cmp(b_name),
                });
                for (val, count) in field_vec {
                    detail.step(format!("{val} -> {count}"));
                }
            }
        }

        Ok(())
    }
}
