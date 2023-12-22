package base

import (
	"fmt"
	"github.com/modern-go/reflect2"
	"reflect"
	"strings"
)

const (
	tag = "avro"
)

func parseStructType(typ reflect2.Type) (s Schema, err error) {
	if typ.Type1().ConvertibleTo(timeType) {
		return NewPrimitiveSchema(Long, NewPrimitiveLogicalSchema(TimestampMicros)), nil
	}
	if typ.Implements(marshalerType) || typ.Implements(unmarshalerType) {
		return NewPrimitiveSchema(Raw, nil), nil
	}
	pkg := typ.Type1().PkgPath()
	pkg = namespace(pkg)
	typeName := typ.Type1().Name()
	processingKey := pkg + "." + typeName
	s = DefaultSchemaCache.getProcessing(processingKey)
	if s != nil {
		return
	}
	rs, rsErr := NewRecordSchema(typeName, pkg, nil)
	if rsErr != nil {
		err = rsErr
		return
	}
	DefaultSchemaCache.addProcessing(processingKey, rs)

	fields, fieldsErr := parseStructFieldTypes(typ)
	if fieldsErr != nil {
		err = fieldsErr
		return
	}
	rs.fields = fields
	s = rs
	return
}

func parseStructFieldTypes(typ reflect2.Type) (fields []*Field, err error) {
	st := typ.(reflect2.StructType)
	num := st.NumField()
	for i := 0; i < num; i++ {
		ft := st.Field(i).(*reflect2.UnsafeStructField)
		if ft.Anonymous() {
			if ft.Type().Kind() == reflect.Ptr && !ft.IsExported() {
				continue
			}
			sub, subErr := parseStructFieldTypes(ft.Type())
			if subErr != nil {
				err = subErr
				return
			}
			fields = append(fields, sub...)
			continue
		}
		if !ft.IsExported() {
			continue
		}
		pname := strings.TrimSpace(ft.Tag().Get(tag))
		if pname == "-" {
			continue
		}
		if pname == "" {
			pname = ft.Name()
		}
		var field *Field
		var fieldErr error
		switch ft.Type().Kind() {
		case reflect.String:
			field, fieldErr = NewField(pname, NewPrimitiveSchema(String, nil))
			break
		case reflect.Bool:
			field, fieldErr = NewField(pname, NewPrimitiveSchema(Boolean, nil))
			break
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Uint8, reflect.Uint16:
			field, fieldErr = NewField(pname, NewPrimitiveSchema(Int, nil))
			break
		case reflect.Int64:
			var fs Schema
			if typ.Type1().ConvertibleTo(timeType) {
				fs = NewPrimitiveSchema(Long, NewPrimitiveLogicalSchema(Duration))
			} else {
				fs = NewPrimitiveSchema(Long, nil)
			}
			field, fieldErr = NewField(pname, fs)
			break
		case reflect.Uint32:
			field, fieldErr = NewField(pname, NewPrimitiveSchema(Long, nil))
			break
		case reflect.Float32:
			field, fieldErr = NewField(pname, NewPrimitiveSchema(Float, nil))
			break
		case reflect.Float64:
			field, fieldErr = NewField(pname, NewPrimitiveSchema(Double, nil))
			break
		case reflect.Uint, reflect.Uint64:
			fs, fsErr := NewFixedSchema("uint", "", 8, NewPrimitiveLogicalSchema(Decimal))
			if fsErr != nil {
				err = fmt.Errorf("avro: parse %s.%s failed, %v", st.String(), ft.Name(), fsErr)
				return
			}
			field, fieldErr = NewField(pname, fs)
			break
		case reflect.Struct:
			if typ.RType() == ft.Type().RType() {
				err = fmt.Errorf("avro: parse %s.%s failed, %v", st.String(), ft.Name(), fmt.Errorf("please use ptr"))
				return
			}
			pkey := makeSchemaName(ft.Type())
			if pkey == "" {
				err = fmt.Errorf("avro: parse %s.%s failed, %v", st.String(), ft.Name(), fmt.Errorf("unsupported type"))
				return
			}

			processing := DefaultSchemaCache.getProcessing(pkey)
			if processing != nil {
				named, isName := processing.(NamedSchema)
				if !isName {
					err = fmt.Errorf("avro: parse %s.%s failed, %v", st.String(), ft.Name(), fmt.Errorf("unsupported type"))
					return
				}
				field, fieldErr = NewField(pname, NewRefSchema(named))
				break
			}
			processing, err = parseStructType(ft.Type())
			if err != nil {
				err = fmt.Errorf("avro: parse %s.%s failed, %v", st.String(), ft.Name(), err)
				return
			}
			field, fieldErr = NewField(pname, processing)
			break
		case reflect.Ptr:
			ptrType := ft.Type().(reflect2.PtrType)
			elemType := ptrType.Elem()
			if elemType.Kind() != reflect.Struct {
				err = fmt.Errorf("avro: parse %s.%s failed, %v", st.String(), ft.Name(), fmt.Errorf("unsupported type"))
				return
			}
			pkey := makeSchemaName(elemType)
			if pkey == "" {
				err = fmt.Errorf("avro: parse %s.%s failed, %v", st.String(), ft.Name(), fmt.Errorf("unsupported type"))
				return
			}
			processing := DefaultSchemaCache.getProcessing(pkey)
			if processing != nil {
				named, isName := processing.(NamedSchema)
				if !isName {
					err = fmt.Errorf("avro: parse %s.%s failed, %v", st.String(), ft.Name(), fmt.Errorf("unsupported type"))
					return
				}
				union, unionErr := NewUnionSchema([]Schema{&NullSchema{}, NewRefSchema(named)})
				if unionErr != nil {
					err = fmt.Errorf("avro: parse %s.%s failed, %v", st.String(), ft.Name(), unionErr)
					return
				}
				field, fieldErr = NewField(pname, union, WithDefault(nil))
				break
			}
			processing, err = parseStructType(elemType)
			if err != nil {
				err = fmt.Errorf("avro: parse %s.%s failed, %v", st.String(), ft.Name(), err)
				return
			}
			union, unionErr := NewUnionSchema([]Schema{&NullSchema{}, processing})
			if unionErr != nil {
				err = fmt.Errorf("avro: parse %s.%s failed, %v", st.String(), ft.Name(), unionErr)
				return
			}
			field, fieldErr = NewField(pname, union, WithDefault(nil))
			break
		case reflect.Slice:
			fs, fsErr := parseSliceType(ft.Type())
			if fsErr != nil {
				err = fmt.Errorf("avro: parse %s.%s failed, %v", st.String(), ft.Name(), fsErr)
				return
			}
			field, fieldErr = NewField(pname, fs)
			break
		case reflect.Array:
			fs, fsErr := parseArrayType(ft.Type())
			if fsErr != nil {
				err = fmt.Errorf("avro: parse %s.%s failed, %v", st.String(), ft.Name(), fsErr)
				return
			}
			field, fieldErr = NewField(pname, fs)
			break
		case reflect.Map:
			fs, fsErr := parseMapType(ft.Type())
			if fsErr != nil {
				err = fmt.Errorf("avro: parse %s.%s failed, %v", st.String(), ft.Name(), fsErr)
				return
			}
			field, fieldErr = NewField(pname, fs)
			break
		default:
			err = fmt.Errorf("avro: parse %s.%s failed, %v", st.String(), ft.Name(), fmt.Errorf("unsupported type"))
			return
		}

		if fieldErr != nil {
			err = fmt.Errorf("avro: parse %s.%s failed, %v", st.String(), ft.Name(), fieldErr)
			return
		}
		for _, f := range fields {
			if f.name == field.name {
				err = fmt.Errorf("avro: parse %s.%s failed, %v", st.String(), ft.Name(), fmt.Errorf("tag name is duplicated"))
				return
			}
		}
		fields = append(fields, field)
	}
	return
}
