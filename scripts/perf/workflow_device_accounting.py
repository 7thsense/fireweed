#!/usr/bin/env python3
"""Account for archived device-monitor-v3 samples; never infer device capacity.

Counter definitions: https://docs.kernel.org/admin-guide/iostats.html
All device measurements are host-wide. Process-present samples omit startup/tail.
"""
import argparse
import gzip
import json
from pathlib import Path


def summarize(rows, device="nvme0n1"):
    active = [row for row in rows if "process_cpu_s" in row]
    if len(active) < 2:
        raise ValueError("need at least two process-present samples")
    for before, after in zip(active, active[1:]):
        if after["monotonic_s"] <= before["monotonic_s"]:
            raise ValueError("non-increasing sample time")
        if len(before[device]) < 17 or len(after[device]) < 17:
            raise ValueError("need modern 17-field disk statistics")
        if any(after[device][i] < before[device][i] for i in range(17) if i != 8):
            raise ValueError("device counter reset/wrap; split the measurement")
    first, last = active[0], active[-1]
    seconds = last["monotonic_s"] - first["monotonic_s"]
    delta = [b - a for a, b in zip(first[device], last[device])]
    ratio = lambda numerator, denominator: numerator / denominator if denominator else None
    return {
        "schema": "workflow-device-accounting/v1",
        "device": device,
        "host_wide_not_process_attribution": True,
        "startup_tail_omitted": True,
        "sampled_seconds": seconds,
        "write_iops": delta[4] / seconds,
        "read_iops": delta[0] / seconds,
        "write_mib_per_second": delta[6] / 2048 / seconds,
        "write_kib_per_request": ratio(delta[6] / 2, delta[4]),
        "mean_write_request_ms": ratio(delta[7], delta[4]),
        "mean_read_request_ms": ratio(delta[3], delta[0]),
        "weighted_mean_inflight": delta[10] / 1000 / seconds,
        "max_sampled_inflight_not_peak_bound": max(row[device][8] for row in active),
        "busy_percent_not_capacity_utilization": delta[9] / 10 / seconds,
        "flush_iops": delta[15] / seconds,
        "mean_flush_request_ms": ratio(delta[16], delta[15]),
        "discard_iops": delta[11] / seconds,
        "discarded_gib": delta[13] / 2097152,
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("samples", type=Path)
    args = parser.parse_args()
    opener = gzip.open if args.samples.suffix == ".gz" else open
    with opener(args.samples, "rt") as source:
        result = summarize([json.loads(line) for line in source])
    result["source"] = str(args.samples)
    print(json.dumps(result, indent=2))
