# TP-005 full-matrix host floors (in progress)
- host: 32 logical CPUs, ~94 GiB RAM
- matrix source commit (slot-a freeze): `25f59301`
- measured cells so far: 33 (still running)
- floors = max(1, 0.25 × observed append items/s) per cell×shape; physics-scaled to this host

| cell | shape | append items/s | claim | finalize | floor (25%) |
|---|---|---:|---:|---:|---:|
| filesystem--memory | minimal | 1755 | 1571 | 1835 | 439 |
| filesystem--memory | record-1k | 1792 | 1562 | 1399 | 448 |
| filesystem--postgres | minimal | 1211 | 1098 | 1224 | 303 |
| filesystem--sqlite | minimal | 1795 | 1863 | 1887 | 449 |
| filesystem--sqlite | record-1k | 1282 | 515 | 1412 | 320 |
| filesystem--turso | minimal | 115 | 735 | 427 | 29 |
| memory--memory | minimal | 163577 | 272774 | 260615 | 40894 |
| memory--postgres | minimal | 5208 | 3594 | 3453 | 1302 |
| memory--postgres | record-1k | 2099 | 2264 | 1889 | 525 |
| memory--sqlite | minimal | 32655 | 34916 | 39351 | 8164 |
| memory--sqlite | record-1k | 2377 | 2252 | 4873 | 594 |
| memory--turso | minimal | 137 | 1429 | 624 | 34 |
| memory--turso | record-1k | 28 | 323 | 294 | 7 |
| postgres--memory | minimal | 15096 | 12029 | 14592 | 3774 |
| postgres--memory | record-1k | 3634 | 11547 | 11253 | 908 |
| postgres--postgres | minimal | 4409 | 1408 | 5600 | 1102 |
| postgres--postgres | record-1k | 2615 | 1399 | 5131 | 654 |
| postgres--sqlite | minimal | 11056 | 10514 | 10818 | 2764 |
| postgres--sqlite | record-1k | 2160 | 2531 | 5314 | 540 |
| postgres--turso | minimal | 160 | 1498 | 707 | 40 |
| postgres--turso | record-1k | 30 | 518 | 456 | 8 |
| s3--memory | minimal | 1916 | 1679 | 1717 | 479 |
| s3--postgres | minimal | 1188 | 711 | 651 | 297 |
| s3--sqlite | minimal | 1246 | 1220 | 963 | 311 |
| s3--turso | minimal | 129 | 467 | 274 | 32 |
| sqlite--memory | minimal | 14172 | 11930 | 9959 | 3543 |
| sqlite--memory | record-1k | 3034 | 8845 | 8469 | 758 |
| sqlite--postgres | minimal | 3878 | 3207 | 3321 | 969 |
| sqlite--postgres | record-1k | 1527 | 2408 | 1959 | 382 |
| sqlite--sqlite | minimal | 5058 | 5682 | 7641 | 1265 |
| sqlite--sqlite | record-1k | 1824 | 2446 | 3857 | 456 |
| sqlite--turso | minimal | 154 | 1397 | 692 | 38 |
| sqlite--turso | record-1k | 34 | 430 | 380 | 8 |

## Class summary (minimal shape, append)

- **filesystem log**: median append 1483 items/s, floor@25% 371
- **memory log**: median append 18932 items/s, floor@25% 4733
- **postgres log**: median append 7732 items/s, floor@25% 1933
- **s3 log**: median append 1217 items/s, floor@25% 304
- **sqlite log**: median append 4468 items/s, floor@25% 1117

Turso projections are ~10–50× slower than sqlite/memory projections on this host at full-tier item counts; floors reflect measured physics, not portable SLAs.
