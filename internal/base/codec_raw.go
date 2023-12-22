package base

import (
	"fmt"
	"github.com/modern-go/reflect2"
	"reflect"
	"unsafe"
)

var (
	marshalerType   = reflect2.TypeOfPtr((*Marshaler)(nil)).Elem()
	unmarshalerType = reflect2.TypeOfPtr((*Unmarshaler)(nil)).Elem()
)

var (
	rawSchema = NewPrimitiveSchema(Bytes, nil)
)

type Marshaler interface {
	MarshalAvro() ([]byte, error)
}

type Unmarshaler interface {
	UnmarshalAvro(p []byte) error
}

func createDecoderOfRaw(_ *frozenConfig, schema Schema, typ reflect2.Type) ValDecoder {
	if typ.Implements(unmarshalerType) && typ.Kind() == reflect.Ptr && schema.Type() == Raw {
		return &rawCodec{
			typ: typ,
		}
	}
	ptrType := reflect2.PtrTo(typ)
	if ptrType.Implements(unmarshalerType) && ptrType.Kind() == reflect.Ptr && schema.Type() == Raw {
		return &referenceDecoder{
			&rawCodec{ptrType},
		}
	}
	return &errorDecoder{err: fmt.Errorf("avro: schema type %s is unsupported", schema.Type())}
}

func createEncoderOfRaw(_ *frozenConfig, schema Schema, typ reflect2.Type) ValEncoder {
	if typ.Implements(marshalerType) && schema.Type() == Raw {
		return &rawCodec{
			typ: typ,
		}
	}
	return &errorEncoder{err: fmt.Errorf("avro: schema type %s is unsupported", schema.Type())}
}

type rawCodec struct {
	typ reflect2.Type
}

func (c rawCodec) Decode(ptr unsafe.Pointer, r *Reader) {
	obj := c.typ.UnsafeIndirect(ptr)
	if reflect2.IsNil(obj) {
		ptrType := c.typ.(*reflect2.UnsafePtrType)
		newPtr := ptrType.Elem().UnsafeNew()
		*((*unsafe.Pointer)(ptr)) = newPtr
		obj = c.typ.UnsafeIndirect(ptr)
	}
	unmarshaler := (obj).(Unmarshaler)
	b := r.ReadBytes()
	p := make([]byte, 0)
	decodeErr := Unmarshal(rawSchema, b, &p)
	if decodeErr != nil {
		r.ReportError("MarshalerCodec", decodeErr.Error())
		return
	}
	err := unmarshaler.UnmarshalAvro(p)
	if err != nil {
		r.ReportError("MarshalerCodec", err.Error())
	}
}

func (c rawCodec) Encode(ptr unsafe.Pointer, w *Writer) {
	obj := c.typ.UnsafeIndirect(ptr)
	if c.typ.IsNullable() && reflect2.IsNil(obj) {
		w.WriteBytes(nil)
		return
	}
	marshaler := (obj).(Marshaler)
	b, err := marshaler.MarshalAvro()
	if err != nil {
		w.Error = err
		return
	}
	b, err = Marshal(rawSchema, b)
	if err != nil {
		w.Error = err
		return
	}
	w.WriteBytes(b)
}
