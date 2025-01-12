use datafusion::logical_expr::Operator;
use datafusion::functions_aggregate::expr_fn::{avg, max, min, stddev, sum};
use datafusion::prelude::*;
use serde::Deserialize;

use crate::plans::SchemaDataType;

#[derive(Debug, Deserialize)]
#[allow(non_camel_case_types)]
pub enum Expression<'a> {
    col(&'a str),
    f32(f32),
    f64(f64),
    i32(i32),
    i64(i64),
    str(&'a str),
    abs(Box<Expression<'a>>),
    acos(Box<Expression<'a>>),
    asin(Box<Expression<'a>>),
    atan(Box<Expression<'a>>),
    not(Box<Expression<'a>>),
    eq(Box<[Expression<'a>;2]>),
    ne(Box<[Expression<'a>;2]>),
    gt(Box<[Expression<'a>;2]>),
    gte(Box<[Expression<'a>;2]>),
    lt(Box<[Expression<'a>;2]>),
    lte(Box<[Expression<'a>;2]>),
    add(Box<[Expression<'a>;2]>),
    sub(Box<[Expression<'a>;2]>),
    mul(Box<[Expression<'a>;2]>),
    div(Box<[Expression<'a>;2]>),
    avg(Vec<Expression<'a>>),
    and(Vec<Expression<'a>>),
    or(Vec<Expression<'a>>),
    min(Vec<Expression<'a>>),
    max(Vec<Expression<'a>>),
    sum(Vec<Expression<'a>>),
    stddev(Vec<Expression<'a>>),

    #[serde(rename(deserialize = "true"))]
    is_true(Box<Expression<'a>>),

    #[serde(rename(deserialize = "false"))]
    is_false(Box<Expression<'a>>),

    #[serde(rename(deserialize = "prod"))]
    product(Vec<Expression<'a>>),

    #[serde(rename(deserialize = "mod"))]
    modulus(Box<[Expression<'a>;2]>),

    cast(Box<Expression<'a>>, SchemaDataType),
}

pub fn convert(expr: &Expression) -> Expr
{
    match expr {
        Expression::f32(v)  => lit(*v),
        Expression::f64(v)  => lit(*v),
        Expression::i32(v)  => lit(*v),
        Expression::i64(v)  => lit(*v),
        Expression::str(v) => lit(*v),
        Expression::col(v) => col(format!(r#""{v}""#)),
        Expression::abs(expr)  => abs(convert(expr)),
        Expression::acos(expr) => acos(convert(expr)),
        Expression::asin(expr) => atan(convert(expr)),
        Expression::atan(expr) => atan(convert(expr)),
        Expression::not(expr)  => not(convert(expr)),
        Expression::and(exprs) => exprs.iter().map(convert).reduce(and).unwrap(),
        Expression::or(exprs)  => exprs.iter().map(convert).reduce(or).unwrap(),
        Expression::avg(exprs) => avg(make_array(exprs.iter().map(convert).collect())),
        Expression::min(exprs) => min(make_array(exprs.iter().map(convert).collect())),
        Expression::max(exprs) => max(make_array(exprs.iter().map(convert).collect())),
        Expression::sum(exprs) => sum(make_array(exprs.iter().map(convert).collect())),
        Expression::eq(exprs)  => binary_expr(convert(&exprs[0]), Operator::Eq, convert(&exprs[1])),
        Expression::ne(exprs)  => binary_expr(convert(&exprs[0]), Operator::NotEq, convert(&exprs[1])),
        Expression::gt(exprs)  => binary_expr(convert(&exprs[0]), Operator::Gt, convert(&exprs[1])),
        Expression::gte(exprs) => binary_expr(convert(&exprs[0]), Operator::GtEq, convert(&exprs[1])),
        Expression::lt(exprs)  => binary_expr(convert(&exprs[0]), Operator::Lt, convert(&exprs[1])),
        Expression::lte(exprs) => binary_expr(convert(&exprs[0]), Operator::LtEq, convert(&exprs[1])),
        Expression::add(exprs) => binary_expr(convert(&exprs[0]), Operator::Plus, convert(&exprs[1])),
        Expression::sub(exprs) => binary_expr(convert(&exprs[0]), Operator::Minus, convert(&exprs[1])),
        Expression::mul(exprs) => binary_expr(convert(&exprs[0]), Operator::Multiply, convert(&exprs[1])),
        Expression::div(exprs) => binary_expr(convert(&exprs[0]), Operator::Divide, convert(&exprs[1])),
        
        Expression::is_true(expr)  => is_true(convert(expr)),
        Expression::is_false(expr) => is_false(convert(expr)),
        Expression::stddev(exprs)  => stddev(make_array(exprs.iter().map(convert).collect())),
        Expression::modulus(exprs) => binary_expr(convert(&exprs[0]), Operator::Modulo, convert(&exprs[1])),
        Expression::product(exprs) => exprs.iter().map(convert).reduce(|a, b| a * b).unwrap(),
        Expression::cast(expr, dtype) => try_cast(convert(expr), (*dtype).into()),
    }
}
