package avro

import (
	"github.com/aacfactory/avro/internal/base"
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
