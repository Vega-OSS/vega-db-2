// Package bufpool provides size-class pooled byte buffers to minimize heap allocations.
//
// Ownership rules:
//   - Whoever calls Get must call Release.
//   - Never keep a sub-slice of B after Release.
//   - Never put a buffer into a protobuf message or any structure that outlives the call.
package bufpool
