use super::ExprError;

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
