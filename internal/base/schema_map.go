package base

import (
	"fmt"
	"github.com/modern-go/reflect2"
	"reflect"
)

func parseMapType(typ reflect2.Type) (s Schema, err error) {
	mapType := typ.(reflect2.MapType)
	if mapType.Key().Kind() != reflect.String {
		err = fmt.Errorf("key of map must be string")
		return
	}
	elemSchema, elemErr := parseValueType(mapType.Elem())
	if elemErr != nil {
		err = elemErr
		return
	}
	s = NewMapSchema(elemSchema)
	return
}
