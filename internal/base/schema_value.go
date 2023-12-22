package base

import (
	"fmt"
	"github.com/modern-go/reflect2"
	"reflect"
	"strings"
)

func RegisterSchemaByValue(v any) {
	_, err := ParseValue(v)
	if err != nil {
		panic(err)
	}
}

func ParseValue(v any) (s Schema, err error) {
	typ := reflect2.TypeOf(v)
	if typ.Kind() == reflect.Ptr {
		typ = typ.(reflect2.PtrType).Elem()
	}
	key := makeSchemaName(typ)
	if key == "" {
		err = fmt.Errorf("avro: type %s is unsupported", typ.String())
		return
	}
	s = DefaultSchemaCache.Get(key)
	if s != nil {
		return
	}
	r, doErr, _ := DefaultSchemaCache.processingGroup.Do(key, func() (r any, err error) {
		parsed, parseErr := parseValueType(typ)
		if parseErr != nil {
			err = parseErr
			return
		}
		DefaultSchemaCache.Add(key, parsed)
		r = parsed
		return
	})
	if doErr != nil {
		err = doErr
		return
	}
	s = r.(Schema)
	return
}

func parseValueType(typ reflect2.Type) (s Schema, err error) {
	if typ.Implements(marshalerType) || typ.Implements(unmarshalerType) {
		return NewPrimitiveSchema(Raw, nil), nil
	}
	if reflect2.PtrTo(typ).Implements(unmarshalerType) {
		return NewPrimitiveSchema(Raw, nil), nil
	}
	switch typ.Kind() {
	case reflect.String:
		return NewPrimitiveSchema(String, nil), nil
	case reflect.Bool:
		return NewPrimitiveSchema(Boolean, nil), nil
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Uint8, reflect.Uint16:
		return NewPrimitiveSchema(Int, nil), nil
	case reflect.Int64:
		if typ.Type1().ConvertibleTo(timeType) {
			return NewPrimitiveSchema(Long, NewPrimitiveLogicalSchema(Duration)), nil
		}
		return NewPrimitiveSchema(Long, nil), nil
	case reflect.Uint32:
		return NewPrimitiveSchema(Long, nil), nil
	case reflect.Float32:
		return NewPrimitiveSchema(Float, nil), nil
	case reflect.Float64:
		return NewPrimitiveSchema(Double, nil), nil
	case reflect.Uint, reflect.Uint64:
		return NewFixedSchema("uint", "", 8, NewPrimitiveLogicalSchema(Decimal))
	case reflect.Struct:
		return parseStructType(typ)
	case reflect.Ptr:
		return parsePtrType(typ)
	case reflect.Slice:
		return parseSliceType(typ)
	case reflect.Array:
		return parseArrayType(typ)
	case reflect.Map:
		return parseMapType(typ)
	default:
		if typ.Implements(marshalerType) || typ.Implements(unmarshalerType) {
			return NewPrimitiveSchema(Raw, nil), nil
		}
		if reflect2.PtrTo(typ).Implements(unmarshalerType) {
			return NewPrimitiveSchema(Raw, nil), nil
		}
		return nil, fmt.Errorf("avro: type %s is unsupported", typ.String())
	}
}

func makeSchemaName(typ reflect2.Type) string {
	if typ.Implements(marshalerType) || typ.Implements(unmarshalerType) {
		return namespace(typ.Type1().PkgPath()) + "." + typ.Type1().Name()
	}
	if reflect2.PtrTo(typ).Implements(unmarshalerType) {
		return namespace(typ.Type1().PkgPath()) + "." + typ.Type1().Name()
	}
	switch typ.Kind() {
	case reflect.String:
		return string(String)
	case reflect.Bool:
		return string(Boolean)
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Uint8, reflect.Uint16:
		return string(Int)
	case reflect.Int64:
		if typ.Type1().PkgPath() == "time" && typ.Type1().Name() == "Duration" {
			return string(Long) + "." + string(Duration)
		}
		return string(Long)
	case reflect.Uint32:
		return string(Long)
	case reflect.Float32:
		return string(Float)
	case reflect.Float64:
		return string(Double)
	case reflect.Uint, reflect.Uint64:
		return string(Fixed) + "." + string(Decimal)
	case reflect.Struct:
		if typ.Type1().ConvertibleTo(timeType) {
			return string(Long) + "." + string(TimestampMicros)
		}
		return namespace(typ.Type1().PkgPath()) + "." + typ.Type1().Name()
	case reflect.Ptr:
		elemType := typ.Type1().Elem()
		if elemType.Kind() != reflect.Struct {
			return ""
		}
		elem := makeSchemaName(reflect2.Type2(elemType))
		if elem == "" {
			return ""
		}
		return elem + "_ptr"
	case reflect.Slice:
		if typ.Type1().Elem().Kind() == reflect.Uint8 {
			return string(Bytes)
		}
		elem := makeSchemaName(typ.(reflect2.SliceType).Elem())
		if elem == "" {
			return ""
		}
		return elem + "_slice"
	case reflect.Array:
		elem := makeSchemaName(typ.(reflect2.ArrayType).Elem())
		if elem == "" {
			return ""
		}
		return elem + "_array"
	case reflect.Map:
		mapType := typ.(reflect2.MapType)
		if mapType.Key().Kind() != reflect.String {
			return ""
		}
		elem := makeSchemaName(mapType.Elem())
		if elem == "" {
			return ""
		}
		return elem + "_map"
	default:
		return ""
	}
}

func namespace(pkg string) (v string) {
	v = strings.ReplaceAll(pkg, "/", ".")
	v = strings.ReplaceAll(v, "-", "_")
	return
}
