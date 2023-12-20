package schemas_test

import (
	"github.com/aacfactory/avro/internal/schemas"
	"testing"
	"time"
)

type STR string

type Baz struct {
	S STR `avro:"s"`
}

type Bar struct {
	Id   string `avro:"id"`
	Name string `avro:"name"`
	//Bar  *Bar   `avro:"bar"`
}

type Foo struct {
	String  string        `avro:"string"`
	Boolean bool          `avro:"boolean"`
	Int     int           `avro:"int"`
	Long    int64         `avro:"long"`
	Float   float32       `avro:"float"`
	Double  float64       `avro:"double"`
	Fixed   uint64        `avro:"fixed"`
	Time    time.Time     `avro:"time"`
	Dur     time.Duration `avro:"dur"`
	Bytes   []byte        `avro:"bytes"`
	//Bars    []Bar          `avro:"bars"`
	//Barz    []*Bar         `avro:"barz"`
	//Bazs    map[string]Baz `avro:"bazs"`
}

func TestGet(t *testing.T) {
	schema, schemaErr := schemas.Get(Foo{})
	if schemaErr != nil {
		t.Error(schemaErr)
	}
	t.Log(schema != nil)
}
