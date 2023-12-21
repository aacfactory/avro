package avro

import (
	"github.com/aacfactory/avro/internal/base"
	"unsafe"
)

func Marshal(v any) (p []byte, err error) {
	schema, schemaErr := base.ParseValue(v)
	if schemaErr != nil {
		err = schemaErr
		return
	}
	p, err = base.Marshal(schema, v)
	return
}

func Unmarshal(p []byte, v any) (err error) {
	schema, schemaErr := base.ParseValue(v)
	if schemaErr != nil {
		err = schemaErr
		return
	}
	err = base.Unmarshal(schema, p, v)
	return
}

func Register(v any) {
	base.RegisterSchemaByValue(v)
}

func SchemaOf(v any) (p []byte, err error) {
	s, parseErr := base.ParseValue(v)
	if parseErr != nil {
		err = parseErr
		return
	}
	str := s.String()
	p = unsafe.Slice(unsafe.StringData(str), len(str))
	return
}
