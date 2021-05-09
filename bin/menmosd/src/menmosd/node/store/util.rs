pub struct DynIter<'iter, V> {
    iter: Box<dyn Iterator<Item = V> + 'iter + Send>,
}

impl<'iter, V> Iterator for DynIter<'iter, V> {
    type Item = V;
    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next()
    }
}

impl<'iter, V> DynIter<'iter, V> {
    pub fn new<I>(iter: I) -> Self
    where
        I: Iterator<Item = V> + 'iter + Send,
    {
        Self {
            iter: Box::new(iter),
        }
    }
}
