use serde::{Serialize, Deserialize};
use std::io::Write;
use std::fs::File;
use std::time::{SystemTime, UNIX_EPOCH};
use std::ffi::CString;
use libc::{open, O_CREAT, O_WRONLY, O_TRUNC, mode_t};
use libc::{c_uint, c_int};
use std::os::fd::{RawFd, FromRawFd};
use std::time::Duration;

const MACHINE_ID: u32 = 1234567890;

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct Vector {
    id: u64,
    data: Vec<f64>
}

impl Vector {
    pub fn new(id: u64, data: Vec<f64>) -> Vector {
        Vector{id, data}
    }

    pub fn data(&self) -> &Vec<f64> {
        &self.data
    }

    pub fn id(&self) -> u64 {
        self.id
    }

    fn to_json(&self) -> String {
        format!("{{\"id\": {:?}, \"data\": {:?}, \"type\": \"upsert\"}}\n", self.id, self.data)
    }

    fn create_direct_io_file(path: &str) -> std::io::Result<File> {
        let c_path = CString::new(path).unwrap();
        unsafe {
            let fd: RawFd = open(
                c_path.as_ptr(),
                O_WRONLY | O_CREAT | O_TRUNC | 0o4000 as mode_t as c_int, 0o644 as mode_t as c_uint,
            );

            if fd < 0 {
                return Err(std::io::Error::last_os_error());
            }

            Ok(File::from_raw_fd(fd))
        }
    }
}

impl Write for Vector {
    fn write(&mut self, _: &[u8]) -> std::io::Result<usize> {
        let payload = self.to_json();
        println!("payload: {}", payload);
        let bytes = payload.into_bytes();
        const BLOCK_SIZE: usize = 4096;
        let padded_len = ((bytes.len() + BLOCK_SIZE - 1) / BLOCK_SIZE) * BLOCK_SIZE;
        println!("padding: {}", padded_len);

        let mut padded_bytes = vec![0u8; padded_len];
        padded_bytes[..bytes.len()].copy_from_slice(&bytes);

        let timestamp = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis();
        let file_path = format!("wal/{}-{}.json", timestamp, MACHINE_ID);

        // simulate network call for testing
        std::thread::sleep(Duration::from_millis(100));

        let mut file = Vector::create_direct_io_file(&file_path)?;
        file.write_all(&padded_bytes)?;

        Ok(padded_bytes.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}
