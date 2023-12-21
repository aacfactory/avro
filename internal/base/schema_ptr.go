package base

import (
	"fmt"
	"github.com/modern-go/reflect2"
	"reflect"
)

func parsePtrType(typ reflect2.Type) (s Schema, err error) {
	ptrType := typ.(reflect2.PtrType)
	elemType := ptrType.Elem()
	if elemType.Kind() != reflect.Struct {
		err = fmt.Errorf("avro: parse %s failed, only support ptr struct", typ.String())
		return
	}
	elem, elemErr := parseStructType(elemType)
	if elemErr != nil {
		err = elemErr
		return
	}
	s, err = NewUnionSchema([]Schema{&NullSchema{}, elem})
	return
}
