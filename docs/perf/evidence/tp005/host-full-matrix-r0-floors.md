# TP-005 full-matrix host floors (round 0 complete)

- host: 32 logical CPUs, ~94 GiB RAM, Linux
- matrix freeze: source commit `25f59301` (slot-a)
- tier: full, round 0 only (r1 still in flight)
- 20 cells × 4 shapes = **80 measured**
- **floor = max(1, 0.25 × observed append items/s)** — host-physics scaled, not portable SLAs
- encoding note: mainline now Base64-encodes envelope bytes (fireweed-659490cc); matrix binary is pre-encoding pin

| cell | shape | append | claim | finalize | floor@25% |
|---|---|---:|---:|---:|---:|
| `filesystem--memory` | group-keyed-256 | 378 | 1328 | 1489 | 95 |
| `filesystem--memory` | large-16k | 147 | 185 | 206 | 37 |
| `filesystem--memory` | minimal | 1755 | 1571 | 1835 | 439 |
| `filesystem--memory` | record-1k | 1792 | 1562 | 1399 | 448 |
| `filesystem--postgres` | group-keyed-256 | 406 | 367 | 527 | 102 |
| `filesystem--postgres` | large-16k | 159 | 206 | 216 | 40 |
| `filesystem--postgres` | minimal | 1211 | 1098 | 1224 | 303 |
| `filesystem--postgres` | record-1k | 1517 | 1561 | 1544 | 379 |
| `filesystem--sqlite` | group-keyed-256 | 375 | 854 | 786 | 94 |
| `filesystem--sqlite` | large-16k | 200 | 175 | 204 | 50 |
| `filesystem--sqlite` | minimal | 1795 | 1863 | 1887 | 449 |
| `filesystem--sqlite` | record-1k | 1282 | 515 | 1412 | 320 |
| `filesystem--turso` | group-keyed-256 | 72 | 125 | 118 | 18 |
| `filesystem--turso` | large-16k | 80 | 118 | 148 | 20 |
| `filesystem--turso` | minimal | 115 | 735 | 427 | 29 |
| `filesystem--turso` | record-1k | 46 | 512 | 467 | 11 |
| `memory--memory` | group-keyed-256 | 70164 | 366966 | 359539 | 17541 |
| `memory--memory` | large-16k | 5052 | 144997 | 144691 | 1263 |
| `memory--memory` | minimal | 163577 | 272774 | 260615 | 40894 |
| `memory--memory` | record-1k | 19036 | 206897 | 183303 | 4759 |
| `memory--postgres` | group-keyed-256 | 330 | 573 | 4188 | 83 |
| `memory--postgres` | large-16k | 1042 | 1361 | 1480 | 260 |
| `memory--postgres` | minimal | 5208 | 3594 | 3453 | 1302 |
| `memory--postgres` | record-1k | 2099 | 2264 | 1889 | 525 |
| `memory--sqlite` | group-keyed-256 | 3605 | 3944 | 3731 | 901 |
| `memory--sqlite` | large-16k | 1542 | 1598 | 5711 | 386 |
| `memory--sqlite` | minimal | 32655 | 34916 | 39351 | 8164 |
| `memory--sqlite` | record-1k | 2377 | 2252 | 4873 | 594 |
| `memory--turso` | group-keyed-256 | 54 | 130 | 122 | 13 |
| `memory--turso` | large-16k | 182 | 359 | 450 | 46 |
| `memory--turso` | minimal | 137 | 1429 | 624 | 34 |
| `memory--turso` | record-1k | 28 | 323 | 294 | 7 |
| `postgres--memory` | group-keyed-256 | 11421 | 18092 | 17454 | 2855 |
| `postgres--memory` | large-16k | 849 | 1729 | 2204 | 212 |
| `postgres--memory` | minimal | 15096 | 12029 | 14592 | 3774 |
| `postgres--memory` | record-1k | 3634 | 11547 | 11253 | 908 |
| `postgres--postgres` | group-keyed-256 | 1497 | 2242 | 2869 | 374 |
| `postgres--postgres` | large-16k | 627 | 832 | 1632 | 157 |
| `postgres--postgres` | minimal | 4409 | 1408 | 5600 | 1102 |
| `postgres--postgres` | record-1k | 2615 | 1399 | 5131 | 654 |
| `postgres--sqlite` | group-keyed-256 | 2344 | 2794 | 3324 | 586 |
| `postgres--sqlite` | large-16k | 363 | 1068 | 1972 | 91 |
| `postgres--sqlite` | minimal | 11056 | 10514 | 10818 | 2764 |
| `postgres--sqlite` | record-1k | 2160 | 2531 | 5314 | 540 |
| `postgres--turso` | group-keyed-256 | 83 | 180 | 154 | 21 |
| `postgres--turso` | large-16k | 149 | 315 | 381 | 37 |
| `postgres--turso` | minimal | 160 | 1498 | 707 | 40 |
| `postgres--turso` | record-1k | 30 | 518 | 456 | 8 |
| `s3--memory` | group-keyed-256 | 992 | 1482 | 1470 | 248 |
| `s3--memory` | large-16k | 234 | 250 | 269 | 58 |
| `s3--memory` | minimal | 1916 | 1679 | 1717 | 479 |
| `s3--memory` | record-1k | 2015 | 2159 | 2248 | 504 |
| `s3--postgres` | group-keyed-256 | 391 | 225 | 274 | 98 |
| `s3--postgres` | large-16k | 191 | 240 | 227 | 48 |
| `s3--postgres` | minimal | 1188 | 711 | 651 | 297 |
| `s3--postgres` | record-1k | 852 | 1286 | 1448 | 213 |
| `s3--sqlite` | group-keyed-256 | 1163 | 1270 | 1362 | 291 |
| `s3--sqlite` | large-16k | 216 | 189 | 230 | 54 |
| `s3--sqlite` | minimal | 1246 | 1220 | 963 | 311 |
| `s3--sqlite` | record-1k | 1671 | 1493 | 2028 | 418 |
| `s3--turso` | group-keyed-256 | 45 | 112 | 141 | 11 |
| `s3--turso` | large-16k | 108 | 145 | 159 | 27 |
| `s3--turso` | minimal | 129 | 467 | 274 | 32 |
| `s3--turso` | record-1k | 42 | 305 | 290 | 10 |
| `sqlite--memory` | group-keyed-256 | 6024 | 6119 | 6999 | 1506 |
| `sqlite--memory` | large-16k | 524 | 2008 | 1904 | 131 |
| `sqlite--memory` | minimal | 14172 | 11930 | 9959 | 3543 |
| `sqlite--memory` | record-1k | 3034 | 8845 | 8469 | 758 |
| `sqlite--postgres` | group-keyed-256 | 550 | 603 | 3528 | 138 |
| `sqlite--postgres` | large-16k | 358 | 794 | 820 | 90 |
| `sqlite--postgres` | minimal | 3878 | 3207 | 3321 | 969 |
| `sqlite--postgres` | record-1k | 1527 | 2408 | 1959 | 382 |
| `sqlite--sqlite` | group-keyed-256 | 2072 | 2094 | 2313 | 518 |
| `sqlite--sqlite` | large-16k | 403 | 987 | 1384 | 101 |
| `sqlite--sqlite` | minimal | 5058 | 5682 | 7641 | 1265 |
| `sqlite--sqlite` | record-1k | 1824 | 2446 | 3857 | 456 |
| `sqlite--turso` | group-keyed-256 | 67 | 129 | 138 | 17 |
| `sqlite--turso` | large-16k | 138 | 238 | 265 | 35 |
| `sqlite--turso` | minimal | 154 | 1397 | 692 | 38 |
| `sqlite--turso` | record-1k | 34 | 430 | 380 | 8 |

## Physics notes (this host)

### Median append items/s by log (minimal)

- **filesystem**: 1483 (floor 371)
- **memory**: 18932 (floor 4733)
- **postgres**: 7732 (floor 1933)
- **s3**: 1217 (floor 304)
- **sqlite**: 4468 (floor 1117)

### Median append items/s by projection (minimal)

- **memory**: 14172 (floor 3543)
- **postgres**: 3878 (floor 969)
- **sqlite**: 5058 (floor 1265)
- **turso**: 137 (floor 34)

Turso is the slowest projection class (~50–150 items/s append on minimal at full tier); memory and sqlite projections are 10–1000× faster. Floors track measured physics so Turso is not held to memory--memory SLAs.
