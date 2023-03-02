use super::ExprError;
use std::fmt::Display;

#[derive(Clone, PartialEq, Eq)]
/// Functions that reduce an entire column to a single value.
pub enum AggregateFunction {
    Count,
    Max,
    Sum,
}

impl AggregateFunction {
    /// Get an aggregation function by name.
    pub fn from_name(name: &str) -> Result<Self, ExprError> {
        match name.to_lowercase().as_str() {
            "count" => Ok(Self::Count),
            "max" => Ok(Self::Max),
            "sum" => Ok(Self::Sum),
            _ => Err(ExprError::UnknownAggregateFunction(name.to_owned())),
        }
    }
}

impl Display for AggregateFunction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Agg({})",
            match self {
                AggregateFunction::Count => "count",
                AggregateFunction::Max => "max",
                AggregateFunction::Sum => "sum",
            }
        )
    }
}
