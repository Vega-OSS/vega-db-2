# VegaDB API Reference

VegaDB provides dual access APIs: a high-performance gRPC interface and an idiomatic HTTP/REST interface.

## HTTP / REST API

### Routes

| Method | Path | Request Body | Response Status | Response Body | Description |
|---|---|---|---|---|---|
| `PUT` | `/v1/kv/{key}` | Binary raw value | `204 No Content` | None | Store key-value pair |
| `GET` | `/v1/kv/{key}` | None | `200 OK` or `404 Not Found` | Binary raw value | Retrieve key value |
| `DELETE` | `/v1/kv/{key}` | None | `204 No Content` | None | Delete key |
| `GET` | `/v1/scan?start=&end=&limit=` | None | `200 OK` | `application/x-ndjson` | Stream keys and values |
| `POST` | `/v1/batch` | JSON batch operations | `204 No Content` | None | Atomic batch operations |
| `GET` | `/healthz` | None | `200 OK` | Text `ok` | Process liveness |
| `GET` | `/readyz` | None | `200 OK` or `503 Service Unavailable` | Text `ok` | Storage readiness |
| `GET` | `/v1/stats` | None | `200 OK` | JSON object | Observability metrics |

### Binary Keys

For arbitrary binary keys containing non-printable characters or slashes, provide the header:
```http
X-Key-Encoding: base64url
```
When this header is set, `{key}` is decoded using RFC 4648 Base64URL encoding without padding.

---

## gRPC API

Service definition: `vega.v1.Vega` in `proto/vega.proto`.

### Methods

- `rpc Put(PutRequest) returns (PutResponse)`
- `rpc Get(GetRequest) returns (GetResponse)`
- `rpc Delete(DeleteRequest) returns (DeleteResponse)`
- `rpc Scan(ScanRequest) returns (stream KV)`
- `rpc BatchWrite(BatchRequest) returns (BatchResponse)`
- `rpc Stats(StatsRequest) returns (StatsResponse)`

### Error Status Codes

- `ErrNotFound` $\to$ `codes.NotFound`
- `ErrInvalid` $\to$ `codes.InvalidArgument`
- `ErrOverloaded` $\to$ `codes.ResourceExhausted`
- `ErrClosed` $\to$ `codes.Unavailable`
- Internal errors $\to$ `codes.Internal`
