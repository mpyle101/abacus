use std::collections::HashMap;
use std::convert::From;
use datafusion::prelude::*;

use crate::plans::{
    self,
    Expression,
    Filter,
    Join,
    Map,
    Select,
    Union
};

#[derive(Clone, Debug)]
pub struct JoinConfig {
    pub join_type: JoinType,
    pub left_cols: Vec<String>,
    pub right_cols: Vec<String>,
}
impl From<&Join<'_>> for JoinConfig {
    fn from(config: &Join) -> JoinConfig
    {
        use plans::JoinType::*;

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
impl From<&Select<'_>> for SelectConfig {
    fn from(config: &Select) -> SelectConfig
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
impl From<&Union<'_>> for UnionConfig {
    fn from(config: &Union) -> UnionConfig
    {
        UnionConfig { distinct: config.distinct.unwrap_or(false) }
    }
}

#[derive(Clone, Debug)]
pub struct FilterConfig {
    pub expr: Expr,
}
impl From<&Filter<'_>> for FilterConfig {
    fn from(config: &Filter) -> FilterConfig
    {
        FilterConfig { expr: convert(&config.expr) }
    }

}

#[derive(Clone, Debug)]
pub struct MapConfig {
    pub exprs: Vec<Expr>,
}
impl From<&Map<'_>> for MapConfig {
    fn from(config: &Map) -> MapConfig
    {
        MapConfig { exprs: config.exprs.iter().map(convert).collect() }
    }
}

fn convert(expr: &Expression) -> Expr
{
    match expr {
        Expression::col(v)     => col(*v),
        Expression::f32(v)     => lit(*v),
        Expression::i32(v)     => lit(*v),
        Expression::abs(expr)  => abs(convert(expr)),
        Expression::avg(expr)  => avg(convert(expr)),
        Expression::acos(expr) => acos(convert(expr)),
        Expression::asin(expr) => atan(convert(expr)),
        Expression::atan(expr) => atan(convert(expr)),
        Expression::not(expr)  => convert(expr).not(),
        Expression::max(exprs) => max(array(exprs.iter().map(convert).collect())),
        Expression::min(exprs) => min(array(exprs.iter().map(convert).collect())),
        Expression::sum(exprs) => sum(array(exprs.iter().map(convert).collect())),
        Expression::gt(exprs)  => {
            let left  = convert(&exprs[0]);
            let right = convert(&exprs[1]);
            left.gt(right)
        },
        Expression::gte(exprs) => {
            let left  = convert(&exprs[0]);
            let right = convert(&exprs[1]);
            left.gt_eq(right)
        },
        Expression::lt(exprs) => {
            let left  = convert(&exprs[0]);
            let right = convert(&exprs[1]);
            left.lt(right)
        },
        Expression::lte(exprs) => {
            let left  = convert(&exprs[0]);
            let right = convert(&exprs[1]);
            left.lt_eq(right)
        },
        Expression::and(exprs) => {
            exprs.iter()
                .map(convert)
                .reduce(|expr, e| expr.and(e))
                .unwrap()
        },
        Expression::or(exprs) => {
            exprs.iter()
                .map(convert)
                .reduce(|expr, e| expr.or(e))
                .unwrap()
        },
        Expression::add(exprs) => {
            let left  = convert(&exprs[0]);
            let right = convert(&exprs[1]);
            left + right
        },
        Expression::sub(exprs) => {
            let left  = convert(&exprs[0]);
            let right = convert(&exprs[1]);
            left - right
        },
        Expression::mul(exprs) => {
            let left  = convert(&exprs[0]);
            let right = convert(&exprs[1]);
            left * right
        },
        Expression::modulus(exprs) => {
            let left  = convert(&exprs[0]);
            let right = convert(&exprs[1]);
            left.modulus(right)
        },
        Expression::product(exprs) => {
            exprs.iter()
                .map(convert)
                .reduce(|expr, e| expr * e)
                .unwrap()
        },

    }
}
