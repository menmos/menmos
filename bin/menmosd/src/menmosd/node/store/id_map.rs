use std::sync::atomic::{AtomicU32, Ordering};

use anyhow::{Context, Result};

use byteorder::{BigEndian, ReadBytesExt};

use sled::IVec;

use crate::node::store::iface::Flush;

/// Generic structure to associate IDs with arbitrary byte slices.
///
/// Supports concurrent use and ID recycling.
pub struct IDMap {
    /// Stores Bytes => ID
    fwd_map: sled::Tree,

    /// Stores ID => Bytes
    rev_map: sled::Tree,

    /// Stores available IDs that are <= next_id
    recycling_store: sled::Tree,

    /// The next ID to allocate.
    next_id: AtomicU32,

    name: String,
}

impl IDMap {
    #[tracing::instrument(name = "id_map_init", skip(db))]
    pub fn new(db: &sled::Db, name: &str) -> Result<Self> {
        let fwd_map = db.open_tree(format!("idmap-{}-fwd", name))?;
        let rev_map = db.open_tree(format!("idmap-{}-rev", name))?;
        let recycling_store = db.open_tree(format!("idmap-{}-recycling", name))?;

        let next_id = match rev_map.last()? {
            Some((k, _)) => k.as_ref().read_u32::<BigEndian>()? + 1,
            None => 0,
        };

        tracing::trace!(next_id = next_id, "init complete");

        Ok(Self {
            fwd_map,
            rev_map,
            recycling_store,
            next_id: AtomicU32::new(next_id),
            name: String::from(name),
        })
    }

    /// Gets the ID associated with an item, assigning it if it doesn't exist.
    pub fn get_or_assign<T: AsRef<[u8]>>(&self, item: T) -> Result<u32> {
        if let Some(i) = self.fwd_map.get(item.as_ref()).unwrap() {
            Ok(i.as_ref().read_u32::<BigEndian>()?)
        } else {
            // Recycle an ID (if possible), else assign a new one.
            let current_id = if let Some((idx_ivec, _)) = self.recycling_store.pop_min()? {
                idx_ivec.as_ref().read_u32::<BigEndian>()?
            } else {
                self.next_id.fetch_add(1, Ordering::SeqCst)
            };

            let current_id_bytes = current_id.to_be_bytes();

            self.fwd_map
                .insert(item.as_ref(), &current_id_bytes.clone())?;

            self.rev_map.insert(current_id_bytes, item.as_ref())?;

            Ok(current_id)
        }
    }

    /// Get the ID corresponding to an item.
    pub fn get<T: AsRef<[u8]>>(&self, item: T) -> Result<Option<u32>> {
        Ok(self
            .fwd_map
            .get(item.as_ref())?
            .map(|idx_ivec| idx_ivec.as_ref().read_u32::<BigEndian>())
            .transpose()?)
    }

    /// Lookup the item associated with an ID.
    pub fn lookup(&self, id: u32) -> Result<Option<IVec>> {
        if let Some(i) = self.rev_map.get(id.to_be_bytes())? {
            Ok(Some(i))
        } else {
            Ok(None)
        }
    }

    /// Delete an item from the ID Map.
    ///
    /// Returns the ID of the item if it was in the map.
    pub fn delete<T: AsRef<[u8]>>(&self, item: T) -> Result<Option<u32>> {
        if let Some(ivec) = self.fwd_map.remove(item.as_ref())? {
            let id = ivec.as_ref().read_u32::<BigEndian>()?;
            self.rev_map.remove(&ivec)?;
            self.recycling_store.insert(ivec, &[])?;
            Ok(Some(id))
        } else {
            Ok(None)
        }
    }

    /// Get the number of allocated and recycled ids.
    ///
    /// This effectively returns (largestID + 1).
    pub fn id_count(&self) -> u32 {
        self.next_id.load(Ordering::SeqCst)
    }

    pub fn recycling_iter(&self) -> impl Iterator<Item = Result<u32>> {
        // This isn't _super_ efficient at query-time, but it makes indexing much quicker.
        // TODO: Improve the datastructure for keeping recycled IDs if this becomes a bottleneck.
        self.recycling_store
            .iter()
            .filter_map(|f| f.ok())
            .map(|p| p.0)
            .map(|id_ivec| {
                let idx = id_ivec.as_ref().read_u32::<BigEndian>()?;
                Ok(idx)
            })
    }

    pub fn clear(&self) -> Result<()> {
        self.next_id.store(0, Ordering::SeqCst);

        self.fwd_map.clear()?;
        self.rev_map.clear()?;
        self.recycling_store.clear()?;

        Ok(())
    }
}

#[async_trait::async_trait]
impl Flush for IDMap {
    #[tracing::instrument(level = "trace", skip(self), fields(name = % self.name))]
    async fn flush(&self) -> Result<()> {
        let (a, b, c) = tokio::join!(
            self.fwd_map.flush_async(),
            self.rev_map.flush_async(),
            self.recycling_store.flush_async()
        );

        a.context("failed to flush forward map")?;
        b.context("failed to flush reverse map")?;
        c.context("failed to flush recycling store")?;

        Ok(())
    }
}
