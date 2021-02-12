pub trait Span {
    /// Returns the span of values covered by this type.
    fn span(&self) -> usize;
}

#[cfg(feature = "bitvec_span")]
impl Span for bitvec::prelude::BitVec {
    fn span(&self) -> usize {
        self.len()
    }
}
