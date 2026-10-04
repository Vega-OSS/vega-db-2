# VegaDB On-Disk Storage Format Specification

This document details the byte-level storage layout for VegaDB files: Value Log segments (`.vlog`), SSTables (`.sst`), and the metadata Manifest (`MANIFEST`).

---

## 1. Value Log Frame Format (`%08d.vlog`)

Every record written to the value log is an append-only frame encoded as follows:

```
+---------------+--------------+----------------+----------------+----------------+------------------+
| CRC32C (4B)   | Flag (1B)    | KeyLen (Varint)| ValLen (Varint)| Key (KeyLen B) | Value (ValLen B) |
+---------------+--------------+----------------+----------------+----------------+------------------+
```

| Field | Size | Description |
|---|---|---|
| `CRC32C` | 4 Bytes (Big Endian) | Castagnoli CRC32 computed over `Flag + KeyLen + ValLen + Key + Value`. |
| `Flag` | 1 Byte | `0x01` = Put (valid payload), `0x02` = Delete (tombstone). |
| `KeyLen` | 1--10 Bytes | Uvarint-encoded length of the key. |
| `ValLen` | 1--10 Bytes | Uvarint-encoded length of the value. |
| `Key` | `KeyLen` Bytes | Raw byte slice of the user key. |
| `Value` | `ValLen` Bytes | Raw byte slice of the user value. |

---

## 2. SSTable Format (`%08d.sst`)

SSTables store keys and 17-byte value pointers, formatted as follows:

```
+-------------------------------------------------------+
| Data Block 0 (4 KiB default + 4B CRC32C)              |
+-------------------------------------------------------+
| Data Block 1 ...                                      |
+-------------------------------------------------------+
| Data Block N                                          |
+-------------------------------------------------------+
| Index Block (varint entries: key + offset + length)   |
+-------------------------------------------------------+
| Bloom Filter Block (k=7 xxhash double-hash bitset)    |
+-------------------------------------------------------+
| Footer (Fixed 48 Bytes)                               |
+-------------------------------------------------------+
```

### 2.1 17-Byte Value Pointer (`record.Record`)
Each key in an SSTable maps to a fixed 17-byte record structure:
```
+-----------+--------------+------------------+------------------+
| Kind (1B) | File ID (4B) | Offset (8B, BE)  | Length (4B, BE)  |
+-----------+--------------+------------------+------------------+
```
- `Kind`: `0x00` = Pointer (points to vLog file and offset), `0x01` = Tombstone.
- `File ID`: 32-bit segment file number.
- `Offset`: 64-bit byte offset within segment.
- `Length`: 32-bit total frame length.

### 2.2 SSTable Footer (Fixed 48 Bytes)
Located at `file_size - 48` bytes:
```
+------------------+------------------+------------------+------------------+--------------------+
| IndexOff (8B, BE)| IndexLen (8B, BE)| BloomOff (8B, BE)| BloomLen (8B, BE)| Magic (8B, BE)     |
|                  |                  |                  |                  | 0x5645474153535431 |
+------------------+------------------+------------------+------------------+--------------------+
```
- Magic constant: `0x5645474153535431` ("VEGASST1").

---

## 3. Manifest Format (`MANIFEST`)

The manifest contains the active state serialized as JSON, updated atomically via write-to-temp, fsync, and rename:

```json
{
  "nextFileId": 5,
  "vlogHead": {
    "file": 1,
    "off": 40960
  },
  "tables": [
    {
      "id": 1,
      "level": 0,
      "smallest": "a2V5XzAwMDA=",
      "largest": "a2V5XzAwOTk=",
      "size": 16384
    }
  ]
}
```
- `vlogHead`: Earliest uncompacted vLog position required for recovery replay.
- `nextFileId`: Monotonically incrementing sequence counter for new table IDs.
- `tables`: Active Level 0 and Level 1 SSTables with base64-encoded smallest/largest key boundaries.
