use std::collections::HashMap;
use std::convert::From;
use datafusion::prelude::*;

use crate::plans::{self, Expression};

#[derive(Clone, Debug)]
pub struct JoinConfig {
    pub join_type: JoinType,
    pub left_cols: Vec<String>,
    pub right_cols: Vec<String>,
}
impl From<&plans::Join<'_>> for JoinConfig {
    fn from(config: &plans::Join) -> Self
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
            .map(|&c| c.into())
            .collect();
        let right_cols = config.rt.iter()
            .map(|&c| c.into())
            .collect();
        
            Self { join_type, left_cols, right_cols }
    }
}

#[derive(Clone, Debug)]
pub struct SelectConfig {
    pub columns: Vec<String>,
    pub aliases: HashMap<String, String>,
}
impl From<&plans::Select<'_>> for SelectConfig {
    fn from(config: &plans::Select) -> Self
    {
        let aliases = config.aliases.iter()
            .map(|(&k, &v)| (format!(r#""{k}""#), v.into()))
            .collect();
        let columns = config.columns.iter()
            .map(|&c| format!(r#""{c}""#))
            .collect();

        Self { aliases, columns }
    }
}

#[derive(Clone, Debug)]
pub struct UnionConfig {
    pub distinct: bool,
}
impl From<&plans::Union<'_>> for UnionConfig {
    fn from(config: &plans::Union) -> Self
    {
        Self { distinct: config.distinct.unwrap_or(false) }
    }
}

#[derive(Clone, Debug)]
pub struct FilterConfig {
    pub expr: Expr,
}
impl From<&plans::Filter<'_>> for FilterConfig {
    fn from(config: &plans::Filter) -> Self
    {
        Self { expr: convert(&config.expr) }
    }

}

#[derive(Clone, Debug)]
pub struct MapConfig {
    pub exprs: Vec<Expr>,
}
impl From<&plans::Map<'_>> for MapConfig {
    fn from(config: &plans::Map) -> Self
    {
        Self { exprs: config.exprs.iter().map(convert).collect() }
    }
}

#[derive(Clone, Debug)]
pub struct SortConfig {
    pub exprs: Vec<Expr>,
}
impl From<&plans::Sort<'_>> for SortConfig {
    fn from(config: &plans::Sort) -> Self
    {
        Self { exprs: config.exprs.iter().map(convert).collect() }
    }
}

#[derive(Clone, Debug)]
pub struct SummarizeConfig {
    pub aggr: Vec<Expr>,
    pub group: Vec<Expr>,
}
impl From<&plans::Summarize<'_>> for SummarizeConfig {
    fn from(config: &plans::Summarize) -> Self
    {
        Self {
            aggr: config.aggr.iter().map(convert).collect(),
            group: config.group.iter().map(convert).collect(),
        }
    }
}

fn convert(expr: &Expression) -> Expr
{
    match expr {
        Expression::col(v)     => col(format!(r#""{v}""#)),
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
            left % right
        },
        Expression::product(exprs) => {
            exprs.iter()
                .map(convert)
                .reduce(|expr, e| expr * e)
                .unwrap()
        },

    }
}
