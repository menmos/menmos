use std::collections::LinkedList;

use tokio::sync::Mutex;

#[derive(Debug, Default)]
pub struct AsyncList<T> {
    data: Mutex<LinkedList<T>>,
}

impl<T> AsyncList<T> {
    pub async fn pop_front(&self) -> Option<T> {
        let mut guard = self.data.lock().await;
        guard.pop_front()
    }

    pub async fn pop_back(&self) -> Option<T> {
        let mut guard = self.data.lock().await;
        guard.pop_back()
    }

    pub async fn push_front(&self, v: T) {
        let mut guard = self.data.lock().await;
        guard.push_front(v)
    }

    pub async fn push_back(&self, v: T) {
        let mut guard = self.data.lock().await;
        guard.push_back(v)
    }
}

impl<T> AsyncList<T>
where
    T: Clone,
{
    pub async fn get_all(&self) -> Vec<T> {
        let guard = self.data.lock().await;
        guard.iter().cloned().collect()
    }
}
