#[derive(Clone, PartialEq, Eq)]
/// Functions that reduce an entire column to a single value.
pub enum AggregateFunction {
    Count,
    Max,
    Sum,
}
