use snafu::{ensure, Snafu};

const MAX_JUMP_INSTRUCTIONS: i32 = 5;

#[derive(Debug, Snafu)]
pub enum BufferError {
    #[snafu(display("unexpected end of buffer"))]
    EndOfBuffer,

    #[snafu(display("limit of {} jumps exceeded", limit))]
    TooManyJumps { limit: i32 },

    #[snafu(display("single label exceeds 63 characters in length"))]
    LabelTooLong,
}

type Result<T> = std::result::Result<T, BufferError>;

pub struct BytePacketBuffer {
    pub buf: [u8; 512],
    pub pos: usize,
}

impl BytePacketBuffer {
    pub fn new() -> BytePacketBuffer {
        BytePacketBuffer {
            buf: [0; 512],
            pos: 0,
        }
    }

    pub fn pos(&self) -> usize {
        self.pos
    }

    pub fn step(&mut self, steps: usize) -> Result<()> {
        self.pos += steps;

        Ok(())
    }

    fn seek(&mut self, pos: usize) {
        self.pos = pos;
    }

    pub fn read(&mut self) -> Result<u8> {
        ensure!(self.pos < 512, EndOfBuffer);
        let res = self.buf[self.pos];
        self.pos += 1;

        Ok(res)
    }

    fn get(&mut self, pos: usize) -> Result<u8> {
        ensure!(pos < 512, EndOfBuffer);
        Ok(self.buf[pos])
    }

    pub fn get_range(&mut self, start: usize, len: usize) -> Result<&[u8]> {
        ensure!(start + len < 512, EndOfBuffer);
        Ok(&self.buf[start..start + len as usize])
    }

    pub fn read_u16(&mut self) -> Result<u16> {
        let res = ((self.read()? as u16) << 8) | (self.read()? as u16);
        Ok(res)
    }

    pub fn read_u32(&mut self) -> Result<u32> {
        let res = ((self.read()? as u32) << 24)
            | ((self.read()? as u32) << 16)
            | ((self.read()? as u32) << 8)
            | (self.read()? as u32);

        Ok(res)
    }

    pub fn read_qname(&mut self, outstr: &mut String) -> Result<()> {
        let mut pos = self.pos();
        let mut jumped = false;

        let mut delim = "";
        let mut jumps_performed = 0;
        loop {
            // Dns Packets are untrusted data, so we need to be paranoid. Someone
            // can craft a packet with a cycle in the jump instructions. This guards
            // against such packets.
            ensure!(
                jumps_performed <= MAX_JUMP_INSTRUCTIONS,
                TooManyJumps {
                    limit: MAX_JUMP_INSTRUCTIONS
                }
            );

            let len = self.get(pos)?;

            // A two byte sequence, where the two highest bits of the first byte is
            // set, represents a offset relative to the start of the buffer. We
            // handle this by jumping to the offset, setting a flag to indicate
            // that we shouldn't update the shared buffer position once done.
            if (len & 0xC0) == 0xC0 {
                // When a jump is performed, we only modify the shared buffer
                // position once, and avoid making the change later on.
                if !jumped {
                    self.seek(pos + 2);
                }

                let b2 = self.get(pos + 1)? as u16;
                let offset = (((len as u16) ^ 0xC0) << 8) | b2;
                pos = offset as usize;
                jumped = true;
                jumps_performed += 1;
                continue;
            }

            pos += 1;

            // Names are terminated by an empty label of length 0
            if len == 0 {
                break;
            }

            outstr.push_str(delim);

            let str_buffer = self.get_range(pos, len as usize)?;
            outstr.push_str(&String::from_utf8_lossy(str_buffer).to_lowercase());

            delim = ".";

            pos += len as usize;
        }

        if !jumped {
            self.seek(pos);
        }

        Ok(())
    }

    pub fn write(&mut self, val: u8) -> Result<()> {
        ensure!(self.pos < 512, EndOfBuffer);
        self.buf[self.pos] = val;
        self.pos += 1;
        Ok(())
    }

    pub fn write_u8(&mut self, val: u8) -> Result<()> {
        self.write(val)?;

        Ok(())
    }

    pub fn write_u16(&mut self, val: u16) -> Result<()> {
        self.write((val >> 8) as u8)?;
        self.write((val & 0xFF) as u8)?;

        Ok(())
    }

    pub fn write_u32(&mut self, val: u32) -> Result<()> {
        self.write(((val >> 24) & 0xFF) as u8)?;
        self.write(((val >> 16) & 0xFF) as u8)?;
        self.write(((val >> 8) & 0xFF) as u8)?;
        self.write((val & 0xFF) as u8)?;

        Ok(())
    }

    pub fn write_qname(&mut self, qname: &str) -> Result<()> {
        for label in qname.split('.') {
            let len = label.len();
            ensure!(len <= 0x34, LabelTooLong);

            self.write_u8(len as u8)?;
            for b in label.as_bytes() {
                self.write_u8(*b)?;
            }
        }

        self.write_u8(0)?;

        Ok(())
    }

    pub fn write_bytes(&mut self, bytes: &[u8]) -> Result<()> {
        let l = bytes.len();
        ensure!(self.pos + l < 512, EndOfBuffer);

        let byte_slice = &mut self.buf[self.pos..self.pos + l];
        byte_slice.copy_from_slice(&bytes);
        Ok(())
    }

    fn set(&mut self, pos: usize, val: u8) {
        self.buf[pos] = val;
    }

    pub fn set_u16(&mut self, pos: usize, val: u16) -> Result<()> {
        self.set(pos, (val >> 8) as u8);
        self.set(pos + 1, (val & 0xFF) as u8);

        Ok(())
    }
}
