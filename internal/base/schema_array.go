package base

import (
	"github.com/modern-go/reflect2"
)

func parseArrayType(typ reflect2.Type) (s Schema, err error) {
	s, err = parseSliceType(typ)
	return
}
