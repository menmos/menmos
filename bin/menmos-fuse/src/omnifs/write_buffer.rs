use bytes::BytesMut;

const BUFFER_CHUNK_SIZE: usize = 25 * 1024 * 1024; // 25mb

#[derive(Clone)]
pub struct WriteBuffer {
    pub offset: u64,
    pub data: BytesMut,
}

impl WriteBuffer {
    pub fn new(offset: u64, data: &[u8]) -> Self {
        Self {
            offset,
            data: data.into(),
        }
    }

    pub fn write(&mut self, offset: u64, data: &[u8]) -> bool {
        if self.data.len() >= BUFFER_CHUNK_SIZE {
            return false;
        }

        if offset >= self.offset
            && (offset + data.len() as u64) <= self.offset + self.data.len() as u64
        {
            // We stay within the buffer, we can simply write to it.
            let start_offset = offset - self.offset;
            let buffer_slice =
                &mut self.data[start_offset as usize..(start_offset as usize + data.len())];
            for (l, r) in buffer_slice.iter_mut().zip(data) {
                *l = *r;
            }
            true
        } else if offset < self.offset && (offset + data.len() as u64) >= self.offset {
            if (offset + data.len() as u64) <= self.offset + self.data.len() as u64 {
                // We don't overflow the buffer, only underflow. So we need to prepend zeros.
                let prepended_segment_size = (self.offset - offset) as usize;
                let new_buffer_length = self.data.len() + prepended_segment_size;

                let mut new_buffer = BytesMut::with_capacity(new_buffer_length);
                new_buffer.resize(new_buffer_length, 0);

                // Copy the old buffer in the new one.
                for (l, r) in new_buffer[prepended_segment_size..]
                    .iter_mut()
                    .zip(&self.data)
                {
                    *l = *r;
                }

                // Then we write the new buffer.
                for (l, r) in new_buffer[0..data.len()].iter_mut().zip(data) {
                    *l = *r
                }

                self.data = new_buffer;
            } else {
                // We underflow *and* overflow, this is easy.
                self.data = BytesMut::from(data);
            }

            self.offset = offset;
            true
        } else if offset > self.offset
            && offset <= (self.offset + self.data.len() as u64)
            && (offset + data.len() as u64) > (self.offset + self.data.len() as u64)
        {
            // We overflow the buffer, we need to extend it.
            let overflow_amount =
                (offset + data.len() as u64) - (self.offset + self.data.len() as u64);
            let new_buffer_size = self.data.len() + overflow_amount as usize;
            debug_assert!(new_buffer_size > self.data.len());
            self.data.resize(new_buffer_size, 0);

            // After extending we're free to write.
            for (l, r) in self.data[(offset - self.offset) as usize..]
                .iter_mut()
                .zip(data)
            {
                *l = *r;
            }
            true
        } else {
            false
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sequential_write() {
        let mut buf = WriteBuffer::new(0, &vec![1, 2, 3, 4]);

        assert!(buf.write(4, &vec![5, 6, 7, 8]));
        assert!(buf.write(8, &vec![9, 10, 11, 12]));
        assert!(buf.write(12, &vec![13, 14, 15, 16]));
        assert_eq!(
            &buf.data,
            &vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16]
        );
        assert_eq!(buf.offset, 0);
    }

    #[test]
    fn discontinuous_overflow_write() {
        let mut buf = WriteBuffer::new(0, &vec![1, 2, 3, 4]);
        assert!(!buf.write(5, &vec! {5, 6, 7, 8}));
        assert_eq!(&buf.data, &vec![1, 2, 3, 4]);
    }

    #[test]
    fn discontinous_underflow_write() {
        let mut buf = WriteBuffer::new(10, &vec![1, 2, 3]);
        assert!(!buf.write(6, &vec![4, 5, 6]));
        assert_eq!(&buf.data, &vec![1, 2, 3]);
    }

    #[test]
    fn continuous_underflow_write() {
        let mut buf = WriteBuffer::new(10, &vec![1, 2, 3]);
        assert!(buf.write(7, &vec![4, 5, 6, 7, 8]));
        assert_eq!(&buf.data.as_ref(), &vec![4, 5, 6, 7, 8, 3]);
        assert_eq!(buf.offset, 7);
    }

    #[test]
    fn continuous_overflow_write() {
        let mut buf = WriteBuffer::new(10, &vec![1, 2, 3]);
        assert!(buf.write(11, &vec![4, 5, 6]));
        assert_eq!(&buf.data.as_ref(), &vec![1, 4, 5, 6]);
    }

    #[test]
    fn continuous_full_overwrite() {
        let mut buf = WriteBuffer::new(10, &vec![1, 2, 3]);
        assert!(buf.write(9, &vec![4, 5, 6, 7, 8]));
        assert_eq!(buf.offset, 9);
        assert_eq!(&buf.data.as_ref(), &vec![4, 5, 6, 7, 8]);
    }

    #[test]
    fn interior_write_no_resize() {
        let mut buf = WriteBuffer::new(10, &vec![1, 2, 3, 4]);
        assert!(buf.write(11, &vec![5, 6]));
        assert_eq!(buf.offset, 10);
        assert_eq!(&buf.data.as_ref(), &vec![1, 5, 6, 4]);
    }
}
