package s3store

import (
	"github.com/davidbirdsong/roster"
	"github.com/davidbirdsong/roster/wirev1"
)

// decodeFunc reads the object stored for one service in one owner. Each wire major version the reader supports
// has one, and every one produces the same canonical roster.Endpoint, so nothing past the reader knows which
// version a record was written in.
type decodeFunc func(ownerID, service string, data []byte) ([]roster.Endpoint, []wirev1.Rejection, error)

// defaultDecoders are the wire versions this build reads. A new major adds an entry beside the old ones, and
// the old one stays until no publisher writes it any more.
func defaultDecoders() map[string]decodeFunc {
	return map[string]decodeFunc{wirev1.Major: wirev1.Decode}
}

// writeMajor is the wire version the Store writes.
const writeMajor = wirev1.Major
