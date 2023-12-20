package avro

import (
	"github.com/aacfactory/avro/internal/schemas"
	"github.com/hamba/avro/v2"
)

func Marshal(v any) (p []byte, err error) {
	schema, schemaErr := schemas.Get(v)
	if schemaErr != nil {
		err = schemaErr
		return
	}
	p, err = avro.Marshal(schema, v)
	return
}

func Unmarshal(p []byte, v any) (err error) {
	schema, schemaErr := schemas.Get(v)
	if schemaErr != nil {
		err = schemaErr
		return
	}
	err = avro.Unmarshal(schema, p, v)
	return
}
