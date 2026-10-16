//! Owned-operation infrastructure and disposable external grouping.
//! Synchronous scratch I/O runs inside a backend's owned physical boundary.
use fireweed_core::queue_contract::{QueueError, QueueResult};
use fireweed_engine::queue_admission::{OwnedQueueBufferCharge, QueueReservation};
use fireweed_engine::queue_planner::UniqueIdentity;
use std::cmp::Ordering;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;
use std::sync::Arc;

const WINDOW: usize = 4096;
const INDEX_BYTES: u64 = 80;
fn unknown(error: impl std::fmt::Display) -> QueueError {
    QueueError::OutcomeUnknown(format!("queue scratch: {error}"))
}
fn add(left: u64, right: u64) -> QueueResult<u64> {
    left.checked_add(right)
        .ok_or_else(|| unknown("offset overflow"))
}
mod operation_runtime;
pub use operation_runtime::QueueOperationRuntime;
mod quota;
pub use quota::{OwnedScratchDiskCharge, ScratchDiskQuota};
/// presence; collection offset/length; index offset/length; tuple offset/length;
/// payload offset/length; producer ordinal. Fixed-width descriptors stay on disk.
#[derive(Clone, Copy)]
struct Descriptor([u64; 10]);
impl Descriptor {
    fn encode(self) -> [u8; 80] {
        let mut bytes = [0; 80];
        for (number, destination) in self.0.iter().zip(bytes.as_chunks_mut::<8>().0) {
            destination.copy_from_slice(&number.to_be_bytes());
        }
        bytes
    }
    fn decode(bytes: [u8; 80]) -> Self {
        let mut numbers = [0; 10];
        for (number, source) in numbers.iter_mut().zip(bytes.as_chunks::<8>().0) {
            *number = u64::from_be_bytes(*source);
        }
        Self(numbers)
    }
    fn validate(self, data_bytes: u64, count: u64) -> QueueResult<()> {
        if self.0[0] > 1 || (self.0[0] == 0 && self.0[2] != 0) || self.0[9] >= count {
            return Err(unknown("invalid descriptor header"));
        }
        for pair in [(1, 2), (3, 4), (5, 6), (7, 8)] {
            if add(self.0[pair.0], self.0[pair.1])? > data_bytes {
                return Err(unknown("descriptor outside scratch data"));
            }
        }
        Ok(())
    }
}
fn descriptor(
    file: &mut File,
    position: u64,
    data_bytes: u64,
    count: u64,
) -> QueueResult<Descriptor> {
    let offset = position
        .checked_mul(INDEX_BYTES)
        .ok_or_else(|| unknown("descriptor offset overflow"))?;
    if position >= count {
        return Err(unknown("descriptor outside index"));
    }
    file.seek(SeekFrom::Start(offset)).map_err(unknown)?;
    let mut bytes = [0; 80];
    file.read_exact(&mut bytes).map_err(unknown)?;
    let result = Descriptor::decode(bytes);
    result.validate(data_bytes, count)?;
    Ok(result)
}
fn compare_windows(
    left: (u64, u64),
    right: (u64, u64),
    mut read: impl FnMut(bool, u64, &mut [u8]) -> QueueResult<()>,
) -> QueueResult<Ordering> {
    let common = left.1.min(right.1);
    let mut position = 0;
    let mut left_bytes = [0; WINDOW];
    let mut right_bytes = [0; WINDOW];
    while position < common {
        let length = (common - position).min(WINDOW as u64) as usize;
        read(true, add(left.0, position)?, &mut left_bytes[..length])?;
        read(false, add(right.0, position)?, &mut right_bytes[..length])?;
        let order = left_bytes[..length].cmp(&right_bytes[..length]);
        if order != Ordering::Equal {
            return Ok(order);
        }
        position = add(position, length as u64)?;
    }
    Ok(left.1.cmp(&right.1))
}
fn read_window(file: &mut File, offset: u64, buffer: &mut [u8]) -> QueueResult<()> {
    file.seek(SeekFrom::Start(offset)).map_err(unknown)?;
    file.read_exact(buffer).map_err(unknown)
}
fn compare_segment(data: &mut File, left: (u64, u64), right: (u64, u64)) -> QueueResult<Ordering> {
    compare_windows(left, right, |_, offset, buffer| {
        read_window(data, offset, buffer)
    })
}
fn compare_keys_across(
    left: &mut File,
    a: Descriptor,
    right: &mut File,
    b: Descriptor,
) -> QueueResult<Ordering> {
    let present = a.0[0].cmp(&b.0[0]);
    if present != Ordering::Equal {
        return Ok(present);
    }
    for (offset, length) in [(1, 2), (3, 4), (5, 6)] {
        let order = compare_windows(
            (a.0[offset], a.0[length]),
            (b.0[offset], b.0[length]),
            |is_left, position, buffer| {
                read_window(if is_left { left } else { right }, position, buffer)
            },
        )?;
        if order != Ordering::Equal {
            return Ok(order);
        }
    }
    // Ordinals belong to different producers and are deliberately not compared.
    Ok(Ordering::Equal)
}

fn compare(data: &mut File, left: Descriptor, right: Descriptor) -> QueueResult<Ordering> {
    let present = left.0[0].cmp(&right.0[0]);
    if present != Ordering::Equal {
        return Ok(present);
    }
    for (offset, length) in [(1, 2), (3, 4), (5, 6)] {
        let order = compare_segment(
            data,
            (left.0[offset], left.0[length]),
            (right.0[offset], right.0[length]),
        )?;
        if order != Ordering::Equal {
            return Ok(order);
        }
    }
    Ok(left.0[9].cmp(&right.0[9]))
}
/// Anonymous files close/unlink on operation drop, including failure. File and
/// identity payloads drop before memory/disk guards. No run descriptor Vec exists.
pub struct QueueSpool {
    data: File,
    index: File,
    spare: File,
    token: Arc<()>,
    binding: [u8; 32],
    data_bytes: u64,
    count: u64,
    poisoned: bool,
    disk: OwnedScratchDiskCharge,
    #[cfg(test)]
    fault: Option<TestFault>,
    _memory: OwnedQueueBufferCharge,
}
impl QueueSpool {
    pub fn new(
        directory: &Path,
        quota: Arc<ScratchDiskQuota>,
        reservation: &Arc<QueueReservation>,
    ) -> QueueResult<Self> {
        let memory = reservation.owned_buffer((3 * WINDOW + 8192) as u64)?;
        Ok(Self {
            data: tempfile::tempfile_in(directory).map_err(unknown)?,
            index: tempfile::tempfile_in(directory).map_err(unknown)?,
            spare: tempfile::tempfile_in(directory).map_err(unknown)?,
            token: Arc::new(()),
            binding: [0; 32],
            data_bytes: 0,
            count: 0,
            poisoned: false,
            disk: quota.reserve(0)?,
            #[cfg(test)]
            fault: None,
            _memory: memory,
        })
    }
    pub fn append(&mut self, key: &UniqueIdentity, payload: &[u8]) -> QueueResult<()> {
        self.append_components(
            key.collection.as_deref(),
            &key.index,
            &key.full_sort_key,
            payload,
        )
    }
    fn append_components(
        &mut self,
        collection: Option<&str>,
        index: &str,
        tuple: &[u8],
        payload: &[u8],
    ) -> QueueResult<()> {
        if self.poisoned {
            return Err(unknown("append after poisoned spool"));
        }
        self.poisoned = true;
        self.append_components_checked(collection, index, tuple, payload)?;
        self.poisoned = false;
        Ok(())
    }
    fn append_components_checked(
        &mut self,
        collection: Option<&str>,
        index: &str,
        tuple: &[u8],
        payload: &[u8],
    ) -> QueueResult<()> {
        let present = collection.is_some();
        let collection = collection.unwrap_or("").as_bytes();
        let index = index.as_bytes();
        let total = add(
            add(collection.len() as u64, index.len() as u64)?,
            add(tuple.len() as u64, payload.len() as u64)?,
        )?;
        let count = add(self.count, 1)?;
        let end = add(self.data_bytes, total)?;
        self.disk.grow(add(total, INDEX_BYTES)?)?;
        let collection_offset = self.data_bytes;
        let index_offset = add(collection_offset, collection.len() as u64)?;
        let tuple_offset = add(index_offset, index.len() as u64)?;
        let payload_offset = add(tuple_offset, tuple.len() as u64)?;
        self.data
            .seek(SeekFrom::Start(self.data_bytes))
            .map_err(unknown)?;
        for bytes in [collection, index, tuple, payload] {
            #[cfg(test)]
            if matches!(self.fault, Some(TestFault::PartialAppend)) && !bytes.is_empty() {
                self.data.write_all(&bytes[..1]).map_err(unknown)?;
                return Err(unknown("injected partial append"));
            }
            self.data.write_all(bytes).map_err(unknown)?;
        }
        let entry = Descriptor([
            u64::from(present),
            collection_offset,
            collection.len() as u64,
            index_offset,
            index.len() as u64,
            tuple_offset,
            tuple.len() as u64,
            payload_offset,
            payload.len() as u64,
            self.count,
        ]);
        self.index
            .seek(SeekFrom::Start(
                self.count
                    .checked_mul(INDEX_BYTES)
                    .ok_or_else(|| unknown("index size overflow"))?,
            ))
            .map_err(unknown)?;
        self.index.write_all(&entry.encode()).map_err(unknown)?;
        self.data_bytes = end;
        self.count = count;
        Ok(())
    }
    /// Certifies only the successfully appended scratch image. The driver must
    /// separately complete and close its selected source under the queue fence.
    /// Errors discard the entire owner; sorting is not publication or an ACK.
    pub fn seal(mut self) -> QueueResult<SortedSpool> {
        if self.poisoned {
            return Err(unknown("seal poisoned spool"));
        }
        self.validate_append_image()?;
        self.sort()?;
        Ok(SortedSpool {
            data: self.data,
            index: self.index,
            _spare: self.spare,
            token: self.token,
            binding: self.binding,
            data_bytes: self.data_bytes,
            count: self.count,
            position: 0,
            previous: None,
            poisoned: false,
            _disk: self.disk,
            _memory: self._memory,
        })
    }
    fn validate_append_image(&mut self) -> QueueResult<()> {
        let index_bytes = self
            .count
            .checked_mul(INDEX_BYTES)
            .ok_or_else(|| unknown("index size overflow"))?;
        if self.index.metadata().map_err(unknown)?.len() != index_bytes
            || self.data.metadata().map_err(unknown)?.len() != self.data_bytes
        {
            return Err(unknown("scratch append image size mismatch"));
        }
        let mut expected_offset = 0;
        for position in 0..self.count {
            let entry = descriptor(&mut self.index, position, self.data_bytes, self.count)?;
            if entry.0[9] != position
                || entry.0[1] != expected_offset
                || entry.0[3] != add(entry.0[1], entry.0[2])?
                || entry.0[5] != add(entry.0[3], entry.0[4])?
                || entry.0[7] != add(entry.0[5], entry.0[6])?
            {
                return Err(unknown("noncontiguous append descriptor"));
            }
            expected_offset = add(entry.0[7], entry.0[8])?;
        }
        if expected_offset != self.data_bytes {
            return Err(unknown("scratch data coverage mismatch"));
        }
        Ok(())
    }
    fn sort(&mut self) -> QueueResult<()> {
        if self.count < 2 {
            return Ok(());
        }
        self.disk.grow(
            self.count
                .checked_mul(INDEX_BYTES)
                .ok_or_else(|| unknown("merge index size overflow"))?,
        )?;
        let mut width = 1u64;
        while width < self.count {
            self.spare.set_len(0).map_err(unknown)?;
            self.spare.seek(SeekFrom::Start(0)).map_err(unknown)?;
            let mut start = 0;
            while start < self.count {
                let middle = add(start, width)?.min(self.count);
                let end = add(middle, width)?.min(self.count);
                let mut left = start;
                let mut right = middle;
                while left < middle || right < end {
                    let next = if left == middle {
                        let row = descriptor(&mut self.index, right, self.data_bytes, self.count)?;
                        right += 1;
                        row
                    } else if right == end {
                        let row = descriptor(&mut self.index, left, self.data_bytes, self.count)?;
                        left += 1;
                        row
                    } else {
                        let a = descriptor(&mut self.index, left, self.data_bytes, self.count)?;
                        let b = descriptor(&mut self.index, right, self.data_bytes, self.count)?;
                        if compare(&mut self.data, a, b)? != Ordering::Greater {
                            left += 1;
                            a
                        } else {
                            right += 1;
                            b
                        }
                    };
                    #[cfg(test)]
                    if matches!(self.fault, Some(TestFault::PartialSort)) {
                        self.spare.write_all(&next.encode()[..8]).map_err(unknown)?;
                        return Err(unknown("injected partial merge write"));
                    }
                    self.spare.write_all(&next.encode()).map_err(unknown)?;
                }
                start = end;
            }
            if self.spare.metadata().map_err(unknown)?.len()
                != self
                    .count
                    .checked_mul(INDEX_BYTES)
                    .ok_or_else(|| unknown("merge index size overflow"))?
            {
                return Err(unknown("merge index size mismatch"));
            }
            std::mem::swap(&mut self.index, &mut self.spare);
            width = width
                .checked_mul(2)
                .ok_or_else(|| unknown("merge width overflow"))?;
        }
        Ok(())
    }
}
#[derive(Clone, Copy)]
pub enum SpoolComponent {
    Collection,
    Index,
    Tuple,
    Payload,
}
/// Opaque operation-bound metadata; no owned key/payload or exposed offsets.
/// Retaining multiple entries or an entry container requires caller admission.
pub struct SpoolEntry {
    descriptor: Descriptor,
    token: Arc<()>,
}
impl SpoolEntry {
    pub fn lengths(&self) -> (Option<u64>, u64, u64, u64) {
        let d = self.descriptor.0;
        ((d[0] == 1).then_some(d[2]), d[4], d[6], d[8])
    }
}
pub struct SortedSpool {
    data: File,
    index: File,
    _spare: File,
    token: Arc<()>,
    binding: [u8; 32],
    data_bytes: u64,
    count: u64,
    position: u64,
    previous: Option<Descriptor>,
    poisoned: bool,
    _disk: OwnedScratchDiskCharge,
    _memory: OwnedQueueBufferCharge,
}
impl SortedSpool {
    pub fn rewind(&mut self) -> QueueResult<()> {
        if self.poisoned {
            return Err(unknown("rewind poisoned reader"));
        }
        self.position = 0;
        self.previous = None;
        Ok(())
    }
    pub fn next_entry(&mut self) -> QueueResult<Option<SpoolEntry>> {
        if self.poisoned {
            return Err(unknown("read poisoned spool"));
        }
        if self.position == self.count {
            self.poisoned = true;
            let expected = self
                .count
                .checked_mul(INDEX_BYTES)
                .ok_or_else(|| unknown("index EOF size overflow"))?;
            if self.index.metadata().map_err(unknown)?.len() != expected
                || self.data.metadata().map_err(unknown)?.len() != self.data_bytes
            {
                return Err(unknown("scratch EOF image size mismatch"));
            }
            self.poisoned = false;
            return Ok(None);
        }
        self.poisoned = true;
        let row = descriptor(&mut self.index, self.position, self.data_bytes, self.count)?;
        if let Some(previous) = self.previous
            && compare(&mut self.data, previous, row)? != Ordering::Less
        {
            return Err(unknown("scratch tuple/ordinal order mismatch"));
        }
        self.previous = Some(row);
        self.position += 1;
        self.poisoned = false;
        Ok(Some(SpoolEntry {
            descriptor: row,
            token: Arc::clone(&self.token),
        }))
    }
    /// Read into caller-admitted windows, including keys larger than the window.
    pub fn read_component(
        &mut self,
        entry: &SpoolEntry,
        component: SpoolComponent,
        position: u64,
        output: &mut [u8],
    ) -> QueueResult<usize> {
        if self.poisoned {
            return Err(unknown("read poisoned spool"));
        }
        self.poisoned = true;
        if !Arc::ptr_eq(&entry.token, &self.token) {
            return Err(unknown("foreign spool entry"));
        }
        entry.descriptor.validate(self.data_bytes, self.count)?;
        let (offset, length) = match component {
            SpoolComponent::Collection => (1, 2),
            SpoolComponent::Index => (3, 4),
            SpoolComponent::Tuple => (5, 6),
            SpoolComponent::Payload => (7, 8),
        };
        let d = entry.descriptor.0;
        if position > d[length] || (position < d[length] && output.is_empty()) {
            return Err(unknown("invalid read window"));
        }
        let size = (d[length] - position).min(output.len() as u64) as usize;
        self.data
            .seek(SeekFrom::Start(add(d[offset], position)?))
            .map_err(unknown)?;
        self.data.read_exact(&mut output[..size]).map_err(unknown)?;
        self.poisoned = false;
        Ok(size)
    }
}

#[cfg(test)]
#[derive(Clone, Copy)]
enum TestFault {
    PartialAppend,
    PartialSort,
}

#[cfg(test)]
mod tests {
    use super::*;
    use fireweed_engine::queue_admission::QueueAdmission;
    use std::sync::Barrier;
    fn key(collection: Option<&str>, index: &str, bytes: &[u8]) -> UniqueIdentity {
        UniqueIdentity {
            collection: collection.map(str::to_owned),
            index: index.into(),
            full_sort_key: bytes.to_vec(),
        }
    }
    fn component(spool: &mut SortedSpool, entry: &SpoolEntry, part: SpoolComponent) -> Vec<u8> {
        let mut output = Vec::new();
        let mut window = [0; WINDOW];
        loop {
            let bytes = spool
                .read_component(entry, part, output.len() as u64, &mut window)
                .unwrap();
            if bytes == 0 {
                return output;
            }
            output.extend_from_slice(&window[..bytes]);
        }
    }
    fn collect(spool: &mut SortedSpool) -> Vec<(UniqueIdentity, Vec<u8>)> {
        let mut rows = Vec::new();
        while let Some(entry) = spool.next_entry().unwrap() {
            let (collection, _, _, _) = entry.lengths();
            let collection = collection.map(|_| {
                String::from_utf8(component(spool, &entry, SpoolComponent::Collection)).unwrap()
            });
            let index = String::from_utf8(component(spool, &entry, SpoolComponent::Index)).unwrap();
            let full_sort_key = component(spool, &entry, SpoolComponent::Tuple);
            let payload = component(spool, &entry, SpoolComponent::Payload);
            rows.push((
                UniqueIdentity {
                    collection,
                    index,
                    full_sort_key,
                },
                payload,
            ));
        }
        rows
    }
    #[test]
    fn empty_singleton_and_non_power_of_two_exact_sort_replay_with_flat_memory() {
        for count in [0, 1, 3, 1027] {
            let directory = tempfile::tempdir().unwrap();
            let admission = QueueAdmission::new(1, 65536).unwrap();
            let job = Arc::new(admission.reserve(0).unwrap());
            let quota = ScratchDiskQuota::new(16 << 20).unwrap();
            let mut spool = QueueSpool::new(directory.path(), Arc::clone(&quota), &job).unwrap();
            let fixed = admission.usage().unwrap().1;
            let mut expected = Vec::new();
            for ordinal in (0..count).rev() {
                let collection = match ordinal % 5 {
                    0 => None,
                    1 => Some(""),
                    2 => Some("\0"),
                    3 => Some("é"),
                    _ => Some("state"),
                };
                let identity = key(
                    collection,
                    &format!("\0index-{}", ordinal % 11),
                    &[ordinal as u8, (ordinal >> 8) as u8],
                );
                let payload = (ordinal as u64).to_be_bytes().to_vec();
                spool.append(&identity, &payload).unwrap();
                expected.push((identity, payload));
                assert_eq!(admission.usage().unwrap().1, fixed);
            }
            expected.sort_by(|left, right| left.0.cmp(&right.0));
            let mut sorted = spool.seal().unwrap();
            assert_eq!(admission.usage().unwrap().1, fixed);
            assert_eq!(collect(&mut sorted), expected);
            sorted.rewind().unwrap();
            assert_eq!(collect(&mut sorted), expected);
            assert_eq!(admission.usage().unwrap().1, fixed);
            drop(sorted);
            assert_eq!(quota.usage().unwrap(), 0);
            assert_eq!(admission.usage().unwrap().1, 0);
            assert_eq!(std::fs::read_dir(directory.path()).unwrap().count(), 0);
        }
    }
    #[test]
    fn full_key_prefixes_and_payloads_larger_than_window_are_exact_and_stable() {
        let directory = tempfile::tempdir().unwrap();
        let admission = QueueAdmission::new(1, 65536).unwrap();
        let job = Arc::new(admission.reserve(0).unwrap());
        let quota = ScratchDiskQuota::new(8 << 20).unwrap();
        let mut spool = QueueSpool::new(directory.path(), Arc::clone(&quota), &job).unwrap();
        let prefix = "p".repeat(WINDOW * 3 + 9);
        let mut expected = Vec::new();
        for ordinal in [9, 2, 2, 1, 8] {
            let mut tuple = vec![0; WINDOW * 4 + 3];
            tuple.push(ordinal);
            let identity = key(Some(&prefix), &format!("{prefix}{ordinal}"), &tuple);
            let payload = vec![ordinal; WINDOW * 2 + 17];
            spool.append(&identity, &payload).unwrap();
            expected.push((identity, payload));
        }
        expected.sort_by(|left, right| left.0.cmp(&right.0));
        let mut sorted = spool.seal().unwrap();
        assert_eq!(collect(&mut sorted), expected);
        drop(sorted);
        assert_eq!(quota.usage().unwrap(), 0);
    }
    #[test]
    fn duplicate_tuple_tie_order_is_producer_order_not_payload_order() {
        let directory = tempfile::tempdir().unwrap();
        let admission = QueueAdmission::new(1, 65536).unwrap();
        let job = Arc::new(admission.reserve(0).unwrap());
        let quota = ScratchDiskQuota::new(4096).unwrap();
        let mut spool = QueueSpool::new(directory.path(), quota, &job).unwrap();
        let identity = key(None, "u", &[7]);
        for payload in [b"z", b"a", b"m"] {
            spool.append(&identity, payload).unwrap();
        }
        let mut sorted = spool.seal().unwrap();
        assert_eq!(
            collect(&mut sorted)
                .into_iter()
                .map(|row| row.1)
                .collect::<Vec<_>>(),
            vec![b"z".to_vec(), b"a".to_vec(), b"m".to_vec()]
        );
    }
    #[test]
    fn both_index_file_peak_is_reserved_and_quota_failure_is_not_semantic_overflow() {
        let directory = tempfile::tempdir().unwrap();
        let admission = QueueAdmission::new(1, 65536).unwrap();
        let job = Arc::new(admission.reserve(0).unwrap());
        let identity = key(None, "u", &[7]);
        let data_bytes = 4;
        let exact = data_bytes + INDEX_BYTES * 4;
        for limit in [exact - 1, exact] {
            let quota = ScratchDiskQuota::new(limit).unwrap();
            let mut spool = QueueSpool::new(directory.path(), Arc::clone(&quota), &job).unwrap();
            spool.append(&identity, &[]).unwrap();
            spool.append(&identity, &[]).unwrap();
            assert_eq!(quota.usage().unwrap(), data_bytes + INDEX_BYTES * 2);
            let result = spool.seal();
            if limit == exact {
                assert!(result.is_ok());
                assert_eq!(quota.usage().unwrap(), exact);
            } else {
                assert!(matches!(result, Err(QueueError::OutcomeUnknown(_))));
            }
            drop(result);
            assert_eq!(quota.usage().unwrap(), 0);
            assert_eq!(admission.usage().unwrap().1, 0);
        }
    }
    #[test]
    fn shared_disk_quota_competes_atomically_across_owners() {
        let quota = ScratchDiskQuota::new(128).unwrap();
        let ready = Arc::new(Barrier::new(17));
        let release = Arc::new(Barrier::new(17));
        std::thread::scope(|scope| {
            for _ in 0..16 {
                let quota = Arc::clone(&quota);
                let ready = Arc::clone(&ready);
                let release = Arc::clone(&release);
                scope.spawn(move || {
                    let mut charge = quota.reserve(0).unwrap();
                    let _ = charge.grow(64);
                    ready.wait();
                    release.wait();
                });
            }
            ready.wait();
            assert_eq!(quota.usage().unwrap(), 128);
            release.wait();
        });
        assert_eq!(quota.usage().unwrap(), 0);
    }
    #[test]
    fn partial_append_remains_charged_and_poisoned_until_owner_drop() {
        let directory = tempfile::tempdir().unwrap();
        let admission = QueueAdmission::new(1, 65536).unwrap();
        let job = Arc::new(admission.reserve(0).unwrap());
        let quota = ScratchDiskQuota::new(4096).unwrap();
        let mut spool = QueueSpool::new(directory.path(), Arc::clone(&quota), &job).unwrap();
        spool.fault = Some(TestFault::PartialAppend);
        assert!(
            spool
                .append(&key(None, "unique", &[1, 2]), b"payload")
                .is_err()
        );
        assert_eq!(spool.data.metadata().unwrap().len(), 1);
        assert_eq!(quota.usage().unwrap(), 6 + 2 + 7 + INDEX_BYTES);
        assert!(spool.append(&key(None, "u", &[]), &[]).is_err());
        assert!(spool.seal().is_err());
        assert_eq!(quota.usage().unwrap(), 0);
        assert_eq!(admission.usage().unwrap().1, 0);
        assert_eq!(std::fs::read_dir(directory.path()).unwrap().count(), 0);
    }
    #[test]
    fn partial_merge_failure_exposes_no_sealed_handle_and_cleans_guards() {
        let directory = tempfile::tempdir().unwrap();
        let admission = QueueAdmission::new(1, 65536).unwrap();
        let job = Arc::new(admission.reserve(0).unwrap());
        let quota = ScratchDiskQuota::new(4096).unwrap();
        let mut spool = QueueSpool::new(directory.path(), Arc::clone(&quota), &job).unwrap();
        for value in [2, 1, 3] {
            spool.append(&key(None, "u", &[value]), &[]).unwrap();
        }
        spool.fault = Some(TestFault::PartialSort);
        assert!(spool.seal().is_err());
        assert_eq!(quota.usage().unwrap(), 0);
        assert_eq!(admission.usage().unwrap().1, 0);
        assert_eq!(std::fs::read_dir(directory.path()).unwrap().count(), 0);
    }
    #[test]
    fn malformed_or_truncated_append_image_never_seals() {
        for corruption in 0..5 {
            let directory = tempfile::tempdir().unwrap();
            let admission = QueueAdmission::new(1, 65536).unwrap();
            let job = Arc::new(admission.reserve(0).unwrap());
            let quota = ScratchDiskQuota::new(4096).unwrap();
            let mut spool = QueueSpool::new(directory.path(), Arc::clone(&quota), &job).unwrap();
            spool.append(&key(None, "u", &[1]), b"body").unwrap();
            match corruption {
                0 => spool.index.set_len(INDEX_BYTES - 1).unwrap(),
                1 => {
                    spool.index.seek(SeekFrom::End(0)).unwrap();
                    spool.index.write_all(&[0]).unwrap();
                }
                2 => {
                    let mut row =
                        descriptor(&mut spool.index, 0, spool.data_bytes, spool.count).unwrap();
                    row.0[5] = u64::MAX;
                    spool.index.seek(SeekFrom::Start(0)).unwrap();
                    spool.index.write_all(&row.encode()).unwrap();
                }
                3 => {
                    let mut row =
                        descriptor(&mut spool.index, 0, spool.data_bytes, spool.count).unwrap();
                    row.0[9] = 1;
                    spool.index.seek(SeekFrom::Start(0)).unwrap();
                    spool.index.write_all(&row.encode()).unwrap();
                }
                _ => spool.data.set_len(spool.data_bytes - 1).unwrap(),
            }
            assert!(spool.seal().is_err());
            assert_eq!(quota.usage().unwrap(), 0);
            assert_eq!(admission.usage().unwrap().1, 0);
        }
    }
    #[test]
    fn reader_failure_and_foreign_entry_permanently_poison_eof() {
        let directory = tempfile::tempdir().unwrap();
        let admission = QueueAdmission::new(1, 65536).unwrap();
        let job = Arc::new(admission.reserve(0).unwrap());
        let quota = ScratchDiskQuota::new(4096).unwrap();
        let mut a = QueueSpool::new(directory.path(), Arc::clone(&quota), &job).unwrap();
        let mut b = QueueSpool::new(directory.path(), Arc::clone(&quota), &job).unwrap();
        a.append(&key(None, "u", &[1]), b"body").unwrap();
        b.append(&key(None, "u", &[2]), b"body").unwrap();
        let mut a = a.seal().unwrap();
        let mut b = b.seal().unwrap();
        let entry = a.next_entry().unwrap().unwrap();
        assert!(
            b.read_component(&entry, SpoolComponent::Payload, 0, &mut [0; 4])
                .is_err()
        );
        assert!(b.next_entry().is_err());
        assert!(b.rewind().is_err());
        a.data.set_len(a.data_bytes - 1).unwrap();
        assert!(
            a.read_component(&entry, SpoolComponent::Payload, 0, &mut [0; 4])
                .is_err()
        );
        assert!(a.next_entry().is_err());
        assert!(a.rewind().is_err());
        drop(a);
        drop(b);
        assert_eq!(quota.usage().unwrap(), 0);
        assert_eq!(admission.usage().unwrap().1, 0);
    }
    #[test]
    fn blocking_owner_keeps_scratch_and_job_alive_after_caller_drop() {
        let directory = tempfile::tempdir().unwrap();
        let admission = QueueAdmission::new(1, 65536).unwrap();
        let job = Arc::new(admission.reserve(128).unwrap());
        let quota = ScratchDiskQuota::new(4096).unwrap();
        let mut spool = QueueSpool::new(directory.path(), Arc::clone(&quota), &job).unwrap();
        for value in [3, 1, 2] {
            spool.append(&key(None, "u", &[value]), b"owned").unwrap();
        }
        let ready = Arc::new(Barrier::new(2));
        let release = Arc::new(Barrier::new(2));
        let worker_ready = Arc::clone(&ready);
        let worker_release = Arc::clone(&release);
        let worker = std::thread::spawn(move || {
            worker_ready.wait();
            worker_release.wait();
            let mut sorted = spool.seal().unwrap();
            for value in [1, 2, 3] {
                let entry = sorted.next_entry().unwrap().unwrap();
                assert_eq!(
                    component(&mut sorted, &entry, SpoolComponent::Tuple),
                    [value]
                );
                assert_eq!(
                    component(&mut sorted, &entry, SpoolComponent::Payload),
                    b"owned"
                );
            }
            assert!(sorted.next_entry().unwrap().is_none());
            drop(sorted);
        });
        ready.wait();
        admission.close().unwrap();
        drop(job);
        assert_eq!(
            admission.usage().unwrap(),
            (1, 128 + (3 * WINDOW + 8192) as u64)
        );
        assert!(quota.usage().unwrap() > 0);
        assert!(matches!(admission.reserve(0), Err(QueueError::Busy)));
        release.wait();
        worker.join().unwrap();
        assert_eq!(admission.usage().unwrap(), (0, 0));
        assert_eq!(quota.usage().unwrap(), 0);
        assert_eq!(std::fs::read_dir(directory.path()).unwrap().count(), 0);
    }
    #[test]
    fn memory_admission_failure_creates_no_files_or_disk_charge() {
        let directory = tempfile::tempdir().unwrap();
        let admission = QueueAdmission::new(1, 64).unwrap();
        let job = Arc::new(admission.reserve(0).unwrap());
        let quota = ScratchDiskQuota::new(4096).unwrap();
        assert!(QueueSpool::new(directory.path(), Arc::clone(&quota), &job).is_err());
        assert_eq!(std::fs::read_dir(directory.path()).unwrap().count(), 0);
        assert_eq!(quota.usage().unwrap(), 0);
    }
}

mod administrative;
mod owned_administrative;
pub use owned_administrative::*;
mod evidence;
pub use administrative::*;

mod worker;
pub use worker::ScratchWorkers;

mod publication;
pub use publication::ResolvedAdministrativePublicationInput;

mod finalization;
pub use finalization::{
    AdministrativeOrdinaryAuthority, FinalizedAdministrativeSemantics,
    finalize_administrative_semantics,
};

mod root;
pub use root::ScratchRoot;

#[cfg(feature = "test-support")]
mod finalization_probe;
#[cfg(feature = "test-support")]
pub use finalization_probe::{FinalizationProbe, FinalizationRelease, FinalizationStage};
