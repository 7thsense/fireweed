## objectlog manifest-head CAS

Bead `pqueue-85bf3c69` changes `SegmentedObjectLog::seal` so a segment is published only by a successful durable `manifest_head/{index}.json` create-only CAS. The older `manifest/{index}.json` object is now a compatibility mirror written after the head slot wins; recovery and reads prefer `manifest_head/` when present and fall back to legacy `manifest/` for existing stores.

Segment bodies written by `seal` now use unique attempt keys under `seg_attempt/` that include epoch, manifest index, first sequence, process id, and a monotonic attempt counter. A failed or stale writer can leave an unreachable attempt object, but it cannot overwrite or delete the live segment object named by the winning manifest-head entry.

`pqueue-c33c367e` is not present in this tracker, so its owner-fence wiring cannot be used as a bounded stale-writer proof. This implementation does not rely on that bead: stale writers are bounded by the permanent head CAS slot for the attempted manifest index, and failed attempts are isolated by unique segment keys.
