package schemas

import (
	"github.com/hamba/avro/v2"
)

func init() {
	cache.Set(stringKey, String())
	cache.Set(booleanKey, Boolean())
	cache.Set(intKey, Int())
	cache.Set(longKey, Long())
	cache.Set(fixKey, Fixed())
	cache.Set(floatKey, Float())
	cache.Set(doubleKey, Double())
	cache.Set(bytesKey, Bytes())
	cache.Set(timeKey, Time())
	cache.Set(durationKey, Duration())

	avro.DefaultSchemaCache.Add("long.timestamp-micros", avro.NewPrimitiveSchema(avro.String, avro.NewPrimitiveLogicalSchema(avro.Date)))
	avro.DefaultSchemaCache.Add("long.time-micros", avro.NewPrimitiveSchema(avro.Long, avro.NewPrimitiveLogicalSchema(avro.Duration)))
	//fixed, _ := avro.NewFixedSchema("uint64", "", 8, avro.NewDecimalLogicalSchema(8, 0))
	//avro.DefaultSchemaCache.Add("uint64", fixed)
}
