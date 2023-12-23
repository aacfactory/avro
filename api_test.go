package avro_test

import (
	"encoding/json"
	"github.com/aacfactory/avro"
	"github.com/aacfactory/avro/internal/base"
	"math/big"
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

type Anonymous struct {
	Anonymous string `avro:"anonymous"`
}

type Bar struct {
	String string `avro:"string"`
	Next   *Bar   `avro:"next"`
}

type Foo struct {
	Anonymous
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
	BigInt  *big.Int       `avro:"bigInt"`
	Bar     Bar            `avro:"bar"`
	Baz     *Bar           `avro:"baz"`
	Bars    []Bar          `avro:"bars"`
	Map     map[string]Bar `avro:"map"`
	Any     Any            `avro:"any"`
}

func TestMarshal(t *testing.T) {
	s, parseErr := base.ParseValue(Foo{})
	if parseErr != nil {
		t.Error(parseErr)
		return
	}
	t.Log(s)
	foo := Foo{
		Anonymous: Anonymous{
			Anonymous: "Anonymous",
		},
		String:  "foo",
		Boolean: true,
		Int:     1,
		Long:    2,
		Float:   3.3,
		Double:  4.4,
		Uint:    uint64(5),
		Time:    time.Now(),
		Dur:     10 * time.Hour,
		BigInt:  big.NewInt(12),
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
			p: []byte("any"),
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

func BenchmarkAvro(b *testing.B) {
	// BenchmarkAvro-20         1000000              1054 ns/op             626 B/op	9 allocs/op
	b.ReportAllocs()
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
	}
	for i := 0; i < b.N; i++ {
		p, _ := avro.Marshal(foo)
		_ = avro.Unmarshal(p, &foo)
	}
}

func BenchmarkJson(b *testing.B) {
	// fastjson BenchmarkJson-20          591304              1953 ns/op             889 B/op          19 allocs/op
	// json 	BenchmarkJson-20          260964              4495 ns/op            1218 B/op 		  29 allocs/op
	b.ReportAllocs()
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
	}
	for i := 0; i < b.N; i++ {
		p, _ := json.Marshal(foo)
		_ = json.Unmarshal(p, &foo)
	}
}

type Big struct {
	Int   *big.Int   `avro:"Int"`
	Float *big.Float `avro:"float"`
	Rat   *big.Rat   `avro:"rat"`
}

func TestBigInt(t *testing.T) {
	v := Big{
		Int:   big.NewInt(1),
		Float: big.NewFloat(2),
		Rat:   big.NewRat(1, 3),
	}
	p, err := avro.Marshal(v)
	if err != nil {
		t.Error(err)
		return
	}
	t.Log(string(p))
	v = Big{}
	err = avro.Unmarshal(p, &v)
	if err != nil {
		t.Error(err)
		return
	}
	t.Log(v)
}

func TestMap(t *testing.T) {
	v := map[string]string{"a": "a", "b": "b"}
	p, err := avro.Marshal(v)
	if err != nil {
		t.Error(err)
		return
	}
	t.Log(string(p))
	v = make(map[string]string)
	err = avro.Unmarshal(p, &v)
	if err != nil {
		t.Error(err)
		return
	}
	t.Log(v)
}
