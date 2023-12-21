# avro
avro for go

## Install
```shell
go get github.com/aacfactory/avro
```

## Usage
Set `avro` tag.
```go
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
	Bar     Bar            `avro:"bar"`
	Baz     *Bar           `avro:"baz"`
	Bars    []Bar          `avro:"bars"`
	Map     map[string]Bar `avro:"map"`
}

```
Marshal.
```go
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

	p, encodeErr := base.Marshal(s, foo)
```
Unmarshal
```go
    r := Foo{}
	decodeErr := base.Unmarshal(s, p, &r)
	if decodeErr != nil {
		t.Error(decodeErr)
		return
	}
```

## Note:
`interface` is not supported.
## Benchmark
avro
```
BenchmarkAvro-20         1000000              1054 ns/op             626 B/op	9 allocs/op
```
encoding/json
```
BenchmarkJson-20          260964              4495 ns/op            1218 B/op   29 allocs/op
```
json-iterator/go
```
BenchmarkJson-20          591304              1953 ns/op             889 B/op   19 allocs/op
```