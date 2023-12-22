package avro

import (
	"fmt"
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

type Marshaler interface {
	MarshalAvro() ([]byte, error)
}

type Unmarshaler interface {
	UnmarshalAvro(p []byte) error
}

type RawMessage []byte

func (r *RawMessage) UnmarshalAvro(p []byte) error {
	*r = p
	return nil
}

func (r RawMessage) MarshalAvro() ([]byte, error) {
	return r, nil
}

func MustMarshal(v any) (p []byte) {
	b, err := Marshal(v)
	if err != nil {
		panic(fmt.Errorf("avro: marshal failed, %v", err))
		return
	}
	p = b
	return
}

func MustUnmarshal(p []byte, v any) {
	err := Unmarshal(p, v)
	if err != nil {
		panic(fmt.Errorf("avro: unmarshal failed, %v", err))
		return
	}
	return
}
