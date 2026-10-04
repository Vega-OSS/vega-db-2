package encoding

import "hash/crc32"

var castagnoliTable = crc32.MakeTable(crc32.Castagnoli)

// ChecksumC returns the CRC32 checksum of data using the Castagnoli polynomial.
func ChecksumC(data []byte) uint32 {
	return crc32.Checksum(data, castagnoliTable)
}
