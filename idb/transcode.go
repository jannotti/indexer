package idb

import (
	"fmt"

	"github.com/algorand/go-codec/codec"
)

// Stringify converts an object into something that can be encoded.
func Stringify(ob interface{}) interface{} {
	switch v := ob.(type) {
	case map[interface{}]interface{}:
		return stringifyMap(v)
	case []interface{}:
		for i := range v {
			v[i] = Stringify(v[i])
		}
		return v
	default:
		return ob
	}
}

// modifes ob IN PLACE
func stringifyMap(ob map[interface{}]interface{}) map[interface{}]interface{} {
	out := make(map[interface{}]interface{}, len(ob))
	for tk, vv := range ob {
		switch k := tk.(type) {
		case string, []byte:
			out[k] = Stringify(vv)
		default:
			nk := fmt.Sprint(tk)
			out[nk] = Stringify(vv)
		}
	}
	return out
}

// JSONOneLine converts an object into JSON
func JSONOneLine(obj interface{}) string {
	var b []byte
	enc := codec.NewEncoderBytes(&b, oneLineJSONCodecHandle)
	enc.MustEncode(obj)
	return string(b)
}

var oneLineJSONCodecHandle *codec.JsonHandle

func init() {
	oneLineJSONCodecHandle = new(codec.JsonHandle)
	oneLineJSONCodecHandle.ErrorIfNoField = true
	oneLineJSONCodecHandle.ErrorIfNoArrayExpand = true
	oneLineJSONCodecHandle.Canonical = true
	oneLineJSONCodecHandle.RecursiveEmptyCheck = true
	oneLineJSONCodecHandle.HTMLCharsAsIs = true
	oneLineJSONCodecHandle.Indent = 0
	oneLineJSONCodecHandle.MapKeyAsString = true
}
