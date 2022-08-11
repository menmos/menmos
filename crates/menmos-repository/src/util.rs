use std::ops::{Bound, Range, RangeBounds};

pub fn bounds_to_range<R: RangeBounds<u64>>(
    r: R,
    fallback_min: u64,
    fallback_max: u64,
) -> Range<u64> {
    let start = match r.start_bound() {
        Bound::Included(i) => *i,
        Bound::Excluded(i) => *i + 1,
        Bound::Unbounded => fallback_min,
    };

    let end = match r.end_bound() {
        Bound::Included(i) => *i + 1,
        Bound::Excluded(i) => *i,
        Bound::Unbounded => fallback_max,
    };

    Range { start, end }
}
