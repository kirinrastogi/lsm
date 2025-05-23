use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use memmap2::{Mmap};
use std::collections::{BTreeSet, BTreeMap};
use std::fs::OpenOptions;
use std::io::{self, BufWriter, Seek, Write, Read};
use std::path::{Path, PathBuf};
use crate::db::vector::Vector;

pub struct LSMTree {
    memtable: BTreeMap<u64, Vector>,
    sstables: Vec<SSTable>,
    directory: PathBuf,
    sstable_size: usize,
    max_open_sstables: usize,
}

struct SSTable {
    mmap: Mmap,
    index: BTreeMap<u64, usize>,
    tombstones: BTreeSet<u64>,
}

impl LSMTree {
    pub fn new(directory: &Path) -> io::Result<Self> {
        std::fs::create_dir_all(directory)?;
        Ok(LSMTree {
            memtable: BTreeMap::new(),
            sstables: Vec::new(),
            directory: directory.to_path_buf(),
            sstable_size: 10,
            max_open_sstables: 10,
        })
    }

    pub fn insert(&mut self, key: u64, value: Vector) -> io::Result<()> {
        self.memtable.insert(key, value);
        if self.memtable.len() >= self.sstable_size {
            self.flush_memtable()?;
        }
        Ok(())
    }

    pub fn get(&self, key: u64) -> Option<Vector> {
        if let Some(value) = self.memtable.get(&key) {
            return Some(value.clone());
        }

        // TODO: check tombstones on delete
        for sstable in self.sstables.iter().rev() {
            if let Some(_) = sstable.tombstones.get(&key) {
                return None;
            }
            if let Some(&offset) = sstable.index.get(&key) {
                let Ok((_, value)) = self.read_value_from_sstable(&sstable.mmap, offset) else {return None};
                       return Some(value);
            }
        }

        None
    }

    pub fn delete(&mut self, key: u64) -> io::Result<()> {
        if let Some(_) = self.memtable.remove(&key) {
            return Ok(());
        }

        for sstable in self.sstables.iter_mut().rev() {
            if let Some(_) = sstable.index.get(&key) {
                sstable.tombstones.insert(key);
                return Ok(());
            }
        }

        Err(io::Error::new(io::ErrorKind::NotFound, format!("Could not find key '{}'", key)))
    }

    // TODO: refactor for any memtable, to re-use in compaction
    fn flush_memtable(&mut self) -> io::Result<()> {
        let sstable_path = self.directory.join(format!("sstable_{}.sdb", self.sstables.len()));
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&sstable_path)?;

        let mut writer = BufWriter::new(&mut file);
        let index = self.write_buffer(&mut writer);

        writer.flush().expect("ERROR flushing");
        drop(writer);

        let mmap = unsafe { Mmap::map(&file)? };
        self.sstables.push( SSTable { mmap, index, tombstones: BTreeSet::new() });
        self.memtable.clear();

        Ok(())
    }

    fn write_buffer<W: Write + Seek>(&mut self, buf: &mut W) -> BTreeMap::<u64, usize> {
        let mut index = BTreeMap::<u64, usize>::new();
        let mut offset = 0;
        for (&key, value) in self.memtable.iter() {
            let entry_offset = offset;
            buf.write_u64::<LittleEndian>(key).expect("ERROR writing u64");
            let serialized = bson::to_vec(value).map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string())).expect("ERROR serializing vector");
            buf.write_u32::<LittleEndian>(serialized.len() as u32).expect("ERROR writing u32");
            buf.write_all(&serialized).expect("ERROR writing vector");
            index.insert(key, entry_offset);
            offset = buf.stream_position().expect("ERROR seeking").try_into().unwrap();
        }

        index
    }

    fn read_value_from_buffer<R: Read + Seek>(&self, buf: &mut R) -> io::Result<(u64, Vector)> {
        let key = buf.read_u64::<LittleEndian>()?;
        let len = buf.read_u32::<LittleEndian>()? as usize;
        let mut serialized = vec![0u8; len];
        buf.read_exact(&mut serialized)?;
        let v: Vector = bson::from_slice(&serialized).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        Ok((key, v))
    }

    fn read_value_from_sstable(&self, mmap: &Mmap, offset: usize) -> io::Result<(u64, Vector)> {
        let mut cursor = io::Cursor::new(&mmap[offset..]);
        let key = cursor.read_u64::<LittleEndian>()?;
        let len = cursor.read_u32::<LittleEndian>()? as usize;
        let mut serialized = vec![0u8; len];
        cursor.read_exact(&mut serialized)?;
        Ok((key, bson::from_slice(&serialized).expect("Unable to deserialize")))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Cursor, SeekFrom};
    use rand::Rng;

    #[test]
    fn test_write_read_buf() {
        let path: PathBuf = "/tmp/lsm".into();
        let mut lsm = LSMTree::new(&path).unwrap();
        let k1: u64 = 1;
        let v1 = Vector::new(k1, vec![0.0, 1.0]);
        let _ = lsm.insert(1, v1.clone());
        let mut buf = Cursor::new(Vec::new());
        let _index = lsm.write_buffer(&mut buf);

        buf.seek(SeekFrom::Start(0)).unwrap();

        let Ok((k2, v2)) = lsm.read_value_from_buffer(&mut buf) else { panic!("could not read from buffer") };

        assert_eq!(k1, k2);
        assert_eq!(v1, v2);
        assert_eq!(v1.id(), v2.id());
        assert_eq!(v1.data(), v2.data());
    }

    #[test]
    fn test_write_read_memtable() {
        let mut rng = rand::rng();
        let path: PathBuf = "/tmp/lsm".into();
        let mut lsm = LSMTree::new(&path).unwrap();
        let v0 = Vector::new(5, vec![rng.random(), rng.random(), rng.random()]);

        for i in 0..8 {
            if i == 5 {
                let _ = lsm.insert(5, v0.clone());
            } else {
                let _ = lsm.insert(i, Vector::new(i, vec![rng.random(), rng.random(), rng.random()]));
            }
        }

        let val = lsm.get(5);
        assert_eq!(val.unwrap().id(), 5);
    }

    #[test]
    fn test_write_read_sstable() {
        let mut rng = rand::rng();
        let path: PathBuf = "/tmp/lsm".into();
        let mut lsm = LSMTree::new(&path).unwrap();
        let v0 = Vector::new(49, vec![rng.random(), rng.random(), rng.random()]);

        for i in 0..100 {
            if i == 49 {
                let _ = lsm.insert(49, v0.clone());
            } else {
                let _ = lsm.insert(i, Vector::new(i, vec![rng.random(), rng.random(), rng.random()]));
            }
        }

        lsm.flush_memtable();

        let val = lsm.get(49);
        assert_eq!(val.unwrap().id(), 49);
    }

    #[test]
    fn test_delete_from_memtable() {
        let path: PathBuf = "/tmp/lsm".into();
        let mut lsm = LSMTree::new(&path).unwrap();
        let k1: u64 = 1;
        let v1 = Vector::new(k1, vec![0.0, 1.0]);
        let _ = lsm.insert(1, v1.clone());
        assert!(lsm.delete(1).is_ok());

        assert_eq!(lsm.memtable.len(), 0);
    }

    #[test]
    fn test_delete_from_sstable() {
        let path: PathBuf = "/tmp/lsm".into();
        let mut lsm = LSMTree::new(&path).unwrap();
        let k1: u64 = 1;
        let v1 = Vector::new(k1, vec![0.0, 1.0]);
        let _ = lsm.insert(1, v1.clone());
        lsm.flush_memtable();
        assert!(lsm.delete(1).is_ok());

        assert_eq!(lsm.memtable.len(), 0);
        assert_eq!(lsm.sstables[0].tombstones.len(), 1);
    }

    #[test]
    fn test_delete_no_key() {
        let path: PathBuf = "/tmp/lsm".into();
        let mut lsm = LSMTree::new(&path).unwrap();
        let result = lsm.delete(1);
        assert!(result.is_err());
        assert!(matches!(result, Err(Error)));
    }

    #[test]
    fn test_delete_prevents_get_memtable() {
        let path: PathBuf = "/tmp/lsm".into();
        let mut lsm = LSMTree::new(&path).unwrap();
        let k1: u64 = 1;
        let v1 = Vector::new(k1, vec![0.0, 1.0]);
        let _ = lsm.insert(1, v1.clone());
        assert!(lsm.delete(1).is_ok());

        assert!(lsm.get(1).is_none());
    }

    #[test]
    fn test_delete_prevents_get_sstable() {
        let path: PathBuf = "/tmp/lsm".into();
        let mut lsm = LSMTree::new(&path).unwrap();
        let k1: u64 = 1;
        let v1 = Vector::new(k1, vec![0.0, 1.0]);
        let _ = lsm.insert(1, v1.clone());
        lsm.flush_memtable();
        assert!(lsm.delete(1).is_ok());

        assert!(lsm.get(1).is_none());
    }
}
