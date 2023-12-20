package avro_test

import (
	"github.com/aacfactory/avro"
	"github.com/aacfactory/json"
	sa "github.com/hamba/avro/v2"
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
	//Baz *Baz `avro:"baz"`
}

type Foo struct {
	String  string         `avro:"string"`
	Boolean bool           `avro:"boolean"`
	Int     int            `avro:"int"`
	Long    int64          `avro:"long"`
	Float   float32        `avro:"float"`
	Double  float64        `avro:"double"`
	Fixed   uint64         `avro:"fixed"`
	Time    time.Time      `avro:"time"`
	Dur     time.Duration  `avro:"dur"`
	Bytes   []byte         `avro:"bytes"`
	Bars    []Bar          `avro:"bars"`
	Barz    []*Bar         `avro:"barz"`
	Bazs    map[string]Baz `avro:"bazs"`
}

func TestMarshal(t *testing.T) {
	p, encodeErr := avro.Marshal(Foo{
		String:  "str",
		Boolean: true,
		Int:     1,
		Long:    11,
		Float:   11.1,
		Double:  22.2,
		Fixed:   3,
		Time:    time.Now(),
		Dur:     8 * time.Second,
		Bytes:   []byte("xxx"),
		Bars:    []Bar{{Id: "id"}},
		Barz:    []*Bar{{Id: "id1"}},
		Bazs:    map[string]Baz{"id": {S: "sss"}},
	})
	if encodeErr != nil {
		t.Error(encodeErr)
		return
	}
	t.Log(len(p))
	foo := Foo{}
	decodeErr := avro.Unmarshal(p, &foo)
	if decodeErr != nil {
		t.Error(decodeErr)
		return
	}
	t.Logf("%+v", foo)
}

func BenchmarkMarshal(b *testing.B) {
	b.ReportAllocs()
	foo := Foo{
		String:  "str",
		Boolean: true,
		Int:     1,
		Long:    11,
		Float:   11.1,
		Double:  22.2,
		Fixed:   3,
		Time:    time.Now(),
		Dur:     8 * time.Second,
		Bytes:   []byte("xxx"),
		Bars:    []Bar{{Id: "id"}},
		Barz:    []*Bar{{Id: "id1"}},
		Bazs:    map[string]Baz{"id": {S: "sss"}},
	}
	for i := 0; i < b.N; i++ {
		p, _ := avro.Marshal(foo)
		_ = avro.Unmarshal(p, &foo)
	}
}

func BenchmarkJson(b *testing.B) {
	b.ReportAllocs()
	foo := Foo{
		String:  "str",
		Boolean: true,
		Int:     1,
		Long:    11,
		Float:   11.1,
		Double:  22.2,
		Fixed:   3,
		Time:    time.Now(),
		Dur:     8 * time.Second,
		Bytes:   []byte("xxx"),
		Bars:    []Bar{{Id: "id"}},
		Barz:    []*Bar{{Id: "id1"}},
		Bazs:    map[string]Baz{"id": {S: "sss"}},
	}
	for i := 0; i < b.N; i++ {
		p, _ := json.Marshal(foo)
		_ = json.Unmarshal(p, &foo)
	}
}

func TestBar(t *testing.T) {
	schema, schemaErr := sa.Parse(`{
	"type": "record",
	"name": "Bar",
	"fields": [
	{"name": "id", "type": "string"},
	{"name": "name", "type": "string"}
]
}`)
	if schemaErr != nil {
		t.Error(schemaErr)
		return
	}
	p, err := sa.Marshal(schema, Bar{
		Id:   "d",
		Name: "1",
	})
	if err != nil {
		t.Error(err)
		return
	}
	t.Log(len(p))
}
