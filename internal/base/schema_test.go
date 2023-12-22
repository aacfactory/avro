package base_test

import (
	"github.com/aacfactory/avro/internal/base"
	"testing"
	"time"
)

type Any struct {
	p []byte
}

func (a *Any) UnmarshalAvro(p []byte) error {
	a.p = p
	return nil
}

func (a Any) MarshalAvro() ([]byte, error) {
	return a.p, nil
}

type Bar struct {
	String string `avro:"string"`
	Next   *Bar   `avro:"next"`
}

type Foo struct {
	String  string         `avro:"string"`
	Boolean bool           `avro:"boolean"`
	Int     int            `avro:"int"`
	Long    int64          `avro:"long"`
	Float   float32        `avro:"float"`
	Double  float64        `avro:"double"`
	Uint    uint64         `avro:"uint"`
	Time    time.Time      `avro:"time"`
	Dur     time.Duration  `avro:"dur"`
	Byte    byte           `avro:"byte"`
	Bytes   []byte         `avro:"bytes"`
	Bar     Bar            `avro:"bar"`
	Baz     *Bar           `avro:"baz"`
	Bars    []Bar          `avro:"bars"`
	Map     map[string]Bar `avro:"map"`
	Any     Any            `avro:"any"`
}

func TestParseValue(t *testing.T) {
	s, parseErr := base.ParseValue(Foo{})
	if parseErr != nil {
		t.Error(parseErr)
		return
	}
	t.Log(s)
	foo := Foo{
		String:  "foo",
		Boolean: true,
		Int:     1,
		Long:    2,
		Float:   3.3,
		Double:  4.4,
		Uint:    uint64(5),
		Time:    time.Now(),
		Dur:     10 * time.Hour,
		Byte:    'B',
		Bytes:   []byte("bytes"),
		Bar: Bar{
			String: "bar",
			Next: &Bar{
				String: "Bar-Next",
				Next:   nil,
			},
		},
		Baz:  nil,
		Bars: []Bar{{String: "bar-1"}},
		Map:  map[string]Bar{"bar2": {String: "bar-2"}},
		Any: Any{
			p: []byte("xxx"),
		},
	}

	p, encodeErr := base.Marshal(s, foo)
	if encodeErr != nil {
		t.Error(encodeErr)
		return
	}
	t.Log(len(p))
	r := Foo{}
	decodeErr := base.Unmarshal(s, p, &r)
	if decodeErr != nil {
		t.Error(decodeErr)
		return
	}
	t.Logf("%+v", r)
}
