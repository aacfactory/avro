package base

import (
	"github.com/modern-go/reflect2"
	"reflect"
)

func parseSliceType(typ reflect2.Type) (s Schema, err error) {
	elemType := typ.(reflect2.SliceType).Elem()
	if elemType.Kind() == reflect.Uint8 {
		s = NewPrimitiveSchema(Bytes, nil)
		return
	}
	elemSchema, elemErr := parseValueType(elemType)
	if elemErr != nil {
		err = elemErr
		return
	}
	s = NewArraySchema(elemSchema)
	return
}
