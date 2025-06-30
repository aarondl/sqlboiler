package boil

import (
	"github.com/volatiletech/null/v8"
)

func GenLoadMapKey(key interface{}) interface{} {
	switch t := key.(type) {
	case []byte:
		return string(t)
	case null.Bytes:
		return string(t.Bytes)
	default:
		return key
	}

}
