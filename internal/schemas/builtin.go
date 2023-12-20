package schemas

import (
	"github.com/hamba/avro/v2"
	"reflect"
	"time"
)

var (
	stringKey   = reflect.TypeOf("")
	booleanKey  = reflect.TypeOf(false)
	intKey      = reflect.TypeOf(0)
	longKey     = reflect.TypeOf(int64(0))
	fixKey      = reflect.TypeOf(uint64(0))
	floatKey    = reflect.TypeOf(float32(0))
	doubleKey   = reflect.TypeOf(float64(0))
	bytesKey    = reflect.TypeOf([]byte{})
	timeKey     = reflect.TypeOf(time.Time{})
	durationKey = reflect.TypeOf(time.Duration(1))
)

func String() Schema {
	value, _ := avro.Parse(`{"name":"string", "type": "string"}`)
	return Schema{
		Key:   "string",
		Kind:  Builtin,
		Value: value,
	}
}

func Boolean() Schema {
	value, _ := avro.Parse(`{"name":"boolean", "type": "boolean"}`)
	return Schema{
		Key:   "boolean",
		Kind:  Builtin,
		Value: value,
	}
}

func Int() Schema {
	value, _ := avro.Parse(`{"name":"int", "type": "int"}`)
	return Schema{
		Key:   "int",
		Kind:  Builtin,
		Value: value,
	}
}

func Long() Schema {
	value, _ := avro.Parse(`{"name":"long", "type": "long"}`)
	return Schema{
		Key:   "long",
		Kind:  Builtin,
		Value: value,
	}
}

func Fixed() Schema {
	value, _ := avro.Parse(`{"name":"fixed", "type": "fixed", "size": 8}`)
	return Schema{
		Key:   "fixed",
		Kind:  Builtin,
		Value: value,
	}
}

func Float() Schema {
	value, _ := avro.Parse(`{"name":"float", "type": "float"}`)
	return Schema{
		Key:   "float",
		Kind:  Builtin,
		Value: value,
	}
}

func Double() Schema {
	value, _ := avro.Parse(`{"name":"double", "type": "double"}`)
	return Schema{
		Key:   "double",
		Kind:  Builtin,
		Value: value,
	}
}

func Bytes() Schema {
	value, _ := avro.Parse(`{"name":"bytes", "type": "bytes"}`)
	return Schema{
		Key:   "bytes",
		Kind:  Builtin,
		Value: value,
	}
}

func Time() Schema {
	value, _ := avro.Parse(`{"name":"time", "type": "long.timestamp-micros"}`)
	return Schema{
		Key:   "long.timestamp-micros",
		Kind:  Builtin,
		Value: value,
	}
}

func Duration() Schema {
	value, _ := avro.Parse(`{"name":"duration", "type": "long.time-micros"}`)
	return Schema{
		Key:   "long.time-micros",
		Kind:  Builtin,
		Value: value,
	}
}
