/// Trait used for computing the precedence order of operations.
///
/// Sets with a higher `Span` value will be prioritized during evaluation.
pub trait Span {
    /// Returns the span of values covered by this type.
    fn span(&self) -> usize;
}

impl Span for bool {
    fn span(&self) -> usize {
        1
    }
}

#[cfg(feature = "bitvec_span")]
impl Span for bitvec::prelude::BitVec {
    fn span(&self) -> usize {
        self.len()
    }
}
