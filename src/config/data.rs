use std::collections::HashMap;
use datafusion::prelude::JoinType;

use crate::plan::{Join, JoinType as ConfigJoinType, Select, Union};

#[derive(Clone, Debug)]
pub struct JoinConfig {
    pub join_type: JoinType,
    pub left_cols: Vec<String>,
    pub right_cols: Vec<String>,
}
impl JoinConfig {
    pub fn new(config: &Join) -> JoinConfig
    {
        use ConfigJoinType::*;

        let join_type = match config.variant {
            full       => JoinType::Full,
            left       => JoinType::Left,
            right      => JoinType::Right,
            inner      => JoinType::Inner,
            left_semi  => JoinType::LeftSemi,
            right_semi => JoinType::RightSemi,
            left_anti  => JoinType::LeftAnti,
            right_anti => JoinType::RightAnti,
        };

        let left_cols = config.lt.iter()
            .map(|c| c.to_string())
            .collect();
        let right_cols = config.rt.iter()
            .map(|c| c.to_string())
            .collect();
        
        JoinConfig { join_type, left_cols, right_cols }
    }
}

#[derive(Clone, Debug)]
pub struct SelectConfig {
    pub columns: Vec<String>,
    pub aliases: HashMap<String, String>,
}
impl SelectConfig {
    pub fn new(config: &Select) -> SelectConfig
    {
        let aliases = config.aliases.iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        let columns = config.columns.iter()
            .map(|c| c.to_string())
            .collect();

        SelectConfig { aliases, columns }
    }
}

#[derive(Clone, Debug)]
pub struct UnionConfig {
    pub distinct: bool,
}
impl UnionConfig {
    pub fn new(config: &Union) -> UnionConfig
    {
        UnionConfig { distinct: config.distinct.unwrap_or(false) }
    }
}