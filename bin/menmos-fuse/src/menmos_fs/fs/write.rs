use crate::write_buffer::WriteBuffer;
use crate::MenmosFS;

use super::Result;

pub struct WriteReply {
    pub written: u32,
}

impl MenmosFS {
    pub async fn write_impl(&self, ino: u64, offset: i64, data: &[u8]) -> Result<WriteReply> {
        log::info!("write i{} {}bytes @ {}", ino, data.len(), offset);

        let mut buffers_guard = self.write_buffers.lock().await;

        if let Some(mut buffer) = buffers_guard.remove(&ino) {
            if !buffer.write(offset as u64, data) {
                self.flush_buffer(ino, buffer).await?;
                buffers_guard.insert(ino, WriteBuffer::new(offset as u64, data));
            } else {
                buffers_guard.insert(ino, buffer);
            }
        } else {
            buffers_guard.insert(ino, WriteBuffer::new(offset as u64, data));
        }

        Ok(WriteReply {
            written: data.len() as u32,
        })
    }
}
