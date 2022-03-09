use std::collections::LinkedList;

use parking_lot::Mutex;

#[derive(Debug)]
pub struct ConcurrentList<T> {
    data: Mutex<LinkedList<T>>,
}

impl<T> Default for ConcurrentList<T> {
    fn default() -> Self {
        Self {
            data: Mutex::new(LinkedList::new()),
        }
    }
}

impl<T> ConcurrentList<T> {
    pub fn pop_front(&self) -> Option<T> {
        let mut guard = self.data.lock();
        guard.pop_front()
    }

    pub fn pop_back(&self) -> Option<T> {
        let mut guard = self.data.lock();
        guard.pop_back()
    }

    pub fn push_front(&self, v: T) {
        let mut guard = self.data.lock();
        guard.push_front(v)
    }

    pub fn push_back(&self, v: T) {
        let mut guard = self.data.lock();
        guard.push_back(v)
    }
}

impl<T> ConcurrentList<T>
where
    T: Clone,
{
    pub fn get_all(&self) -> Vec<T> {
        let guard = self.data.lock();
        guard.iter().cloned().collect()
    }

    /// Fetches the head of the list and swaps it to the back of the list atomically.
    pub fn fetch_swap(&self) -> Option<T> {
        let mut guard = self.data.lock();
        match guard.pop_front() {
            Some(v) => {
                let value_copy = v.clone();
                guard.push_back(v);
                Some(value_copy)
            }
            None => None,
        }
    }
}
