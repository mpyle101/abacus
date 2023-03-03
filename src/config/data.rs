use std::collections::HashMap;
use datafusion::prelude::JoinType;

use crate::plan::{Join, JoinType as ConfigJoinType, Select};

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

        JoinConfig { join_type, left_cols: config.lt.clone(), right_cols: config.rt.clone() }
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
        SelectConfig { aliases: config.aliases.clone(), columns: config.columns.clone() }
    }
}