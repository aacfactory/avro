package schemas

import (
	"context"
	"errors"
	"fmt"
	"github.com/hamba/avro/v2"
	"github.com/valyala/bytebufferpool"
	"golang.org/x/sync/singleflight"
	"reflect"
	"strings"
	"sync"
)

const (
	Builtin Kind = iota
	Struct
	Slice
	Map
)

type Kind int

type Schema struct {
	Key   string
	Kind  Kind
	Value avro.Schema
}

var (
	group = singleflight.Group{}
	cache = new(Cache)
)

type Cache struct {
	values sync.Map
}

func (cache *Cache) Get(rt reflect.Type) (v Schema, has bool) {
	vv, exist := cache.values.Load(rt)
	if !exist {
		return
	}
	v, has = vv.(Schema)
	return
}

func (cache *Cache) Set(rt reflect.Type, v Schema) (has bool) {
	cache.values.Store(rt, v)
	return
}

func Get(v any) (schema avro.Schema, err error) {
	rt := reflect.Indirect(reflect.ValueOf(v)).Type()
	stored, has := cache.Get(rt)
	if has {
		schema = stored.Value
		return
	}
	vv, getErr := get(context.TODO(), rt)
	if getErr != nil {
		err = getErr
		return
	}
	schema = vv.Value
	return
}

func get(ctx context.Context, rt reflect.Type) (schema Schema, err error) {
	stored, has := cache.Get(rt)
	if has {
		schema = stored
		return
	}
	key := fmt.Sprintf("%v", reflect.ValueOf(rt).UnsafePointer())
	vv, doErr, _ := group.Do(key, func() (v interface{}, err error) {
		ctx = context.WithValue(ctx, "local", make(map[string]struct{}))
		var r Schema
		switch rt.Kind() {
		case reflect.String:
			r, _ = cache.Get(stringKey)
			break
		case reflect.Bool:
			r, _ = cache.Get(booleanKey)
			break
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Uint8, reflect.Uint16:
			r, _ = cache.Get(intKey)
			break
		case reflect.Int64:
			pkg := rt.PkgPath()
			name := rt.Name()
			if pkg == "time" && name == "Duration" {
				r, _ = cache.Get(durationKey)
				break
			}
			r, _ = cache.Get(longKey)
			break
		case reflect.Uint32:
			r, _ = cache.Get(longKey)
			break
		case reflect.Float32:
			r, _ = cache.Get(floatKey)
			break
		case reflect.Float64:
			r, _ = cache.Get(durationKey)
			break
		case reflect.Uint64:
			r, _ = cache.Get(floatKey)
			break
		case reflect.Struct:
			p, parseErr := parse(ctx, rt)
			if parseErr != nil {
				err = parseErr
				return
			}
			value, valueErr := avro.ParseBytes(p)
			if valueErr != nil {
				err = valueErr
				return
			}
			sk := fmt.Sprintf("%s.%s", strings.ReplaceAll(rt.PkgPath(), "/", "."), rt.Name())
			r = Schema{
				Key:   sk,
				Kind:  Struct,
				Value: value,
			}
			avro.Register(sk, value)
			break
		case reflect.Ptr:
			elem := rt.Elem()
			if elem.Kind() != reflect.Struct {
				err = fmt.Errorf("%s is not supported", rt.String())
				return
			}
			p, parseErr := parse(ctx, elem)
			if parseErr != nil {
				err = parseErr
				return
			}
			value, valueErr := avro.ParseBytes(p)
			if valueErr != nil {
				err = valueErr
				return
			}
			sk := fmt.Sprintf("%s.%s", strings.ReplaceAll(rt.PkgPath(), "/", "."), rt.Name())
			r = Schema{
				Key:   fmt.Sprintf("%s.%s", strings.ReplaceAll(elem.PkgPath(), "/", "."), elem.Name()),
				Kind:  Struct,
				Value: value,
			}
			avro.Register(sk, value)
			break
		case reflect.Slice:
			p, parseErr := parse(ctx, rt)
			if parseErr != nil {
				err = parseErr
				return
			}
			value, valueErr := avro.ParseBytes(p)
			if valueErr != nil {
				err = valueErr
				return
			}
			r = Schema{
				Key:   "",
				Kind:  Slice,
				Value: value,
			}
			break
		case reflect.Map:
			p, parseErr := parse(ctx, rt)
			if parseErr != nil {
				err = parseErr
				return
			}
			value, valueErr := avro.ParseBytes(p)
			if valueErr != nil {
				err = valueErr
				return
			}
			r = Schema{
				Key:   "",
				Kind:  Map,
				Value: value,
			}
			break
		default:
			err = fmt.Errorf("%s is not supported", rt.String())
			return
		}

		cache.Set(rt, r)
		v = r
		return
	})
	if doErr != nil {
		err = doErr
		return
	}
	schema = vv.(Schema)
	return
}

func parse(ctx context.Context, rt reflect.Type) (p []byte, err error) {
	buf := bytebufferpool.Get()
	defer bytebufferpool.Put(buf)
	switch rt.Kind() {
	case reflect.String:
		_, _ = buf.WriteString(`"string"`)
		p = []byte(buf.String())
		break
	case reflect.Bool:
		_, _ = buf.WriteString(`"boolean"`)
		p = []byte(buf.String())
		break
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Uint8, reflect.Uint16:
		_, _ = buf.WriteString(`"int"`)
		p = []byte(buf.String())
		break
	case reflect.Int64:
		pkg := rt.PkgPath()
		name := rt.Name()
		if pkg == "time" && name == "Duration" {
			_, _ = buf.WriteString(`"long.time-micros"`)
			p = []byte(buf.String())
			break
		}
		_, _ = buf.WriteString(`"long"`)
		p = []byte(buf.String())
		break
	case reflect.Uint32:
		_, _ = buf.WriteString(`"long"`)
		p = []byte(buf.String())
		break
	case reflect.Float32:
		_, _ = buf.WriteString(`"float"`)
		p = []byte(buf.String())
		break
	case reflect.Float64:
		_, _ = buf.WriteString(`"double"`)
		p = []byte(buf.String())
		break
	case reflect.Uint64:
		_, _ = buf.WriteString(`{`)
		_, _ = buf.WriteString(`"name": "uint64", `)
		_, _ = buf.WriteString(`"type": "fixed", `)
		_, _ = buf.WriteString(`"size": 8`)
		_, _ = buf.WriteString(`}`)
		p = []byte(buf.String())
		break
	case reflect.Struct:
		if rt.ConvertibleTo(timeType) {
			_, _ = buf.WriteString(`"long.timestamp-micros"`)
			p = []byte(buf.String())
			break
		}
		sk := fmt.Sprintf("%s.%s", rt.PkgPath(), rt.Name())
		parsing := ctx.Value(sk)
		if parsing != nil {
			_, _ = buf.WriteString(`{`)
			_, _ = buf.WriteString(`"type": "`)
			_, _ = buf.WriteString(strings.ReplaceAll(rt.PkgPath(), "/", ".") + "." + rt.Name())
			_, _ = buf.WriteString(`", `)
			_, _ = buf.WriteString(`"name": "`)
			_, _ = buf.WriteString(rt.Name())
			_, _ = buf.WriteString(`"`)
			_, _ = buf.WriteString(`}`)
			p = []byte(buf.String())
			break
		}
		local := ctx.Value("local").(map[string]struct{})
		_, inLocal := local[sk]
		if inLocal {
			_, _ = buf.WriteString(`{`)
			_, _ = buf.WriteString(`"type": "`)
			_, _ = buf.WriteString(strings.ReplaceAll(rt.PkgPath(), "/", ".") + "." + rt.Name())
			_, _ = buf.WriteString(`", `)
			_, _ = buf.WriteString(`"name": "`)
			_, _ = buf.WriteString(rt.Name())
			_, _ = buf.WriteString(`"`)
			_, _ = buf.WriteString(`}`)
			p = []byte(buf.String())
			break
		}
		_, parsed := cache.Get(rt)
		if parsed {
			_, _ = buf.WriteString(`{`)
			_, _ = buf.WriteString(`"type": "`)
			_, _ = buf.WriteString(strings.ReplaceAll(rt.PkgPath(), "/", ".") + "." + rt.Name())
			_, _ = buf.WriteString(`", `)
			_, _ = buf.WriteString(`"name": "`)
			_, _ = buf.WriteString(rt.Name())
			_, _ = buf.WriteString(`"`)
			_, _ = buf.WriteString(`}`)
			p = []byte(buf.String())
			break
		}
		ctx = context.WithValue(ctx, sk, struct{}{})
		local[sk] = struct{}{}
		_, _ = buf.WriteString(`{`)
		_, _ = buf.WriteString(`"type": "record", `)
		_, _ = buf.WriteString(`"namespace": "`)
		_, _ = buf.WriteString(strings.ReplaceAll(rt.PkgPath(), "/", "."))
		_, _ = buf.WriteString(`", `)
		_, _ = buf.WriteString(`"name": "`)
		_, _ = buf.WriteString(rt.Name())
		_, _ = buf.WriteString(`", `)
		// fields
		fields, fieldsErr := parseStructFields(ctx, rt)
		if fieldsErr != nil {
			err = fmt.Errorf("%s is not supported, %v", rt.String(), fieldsErr)
			return
		}
		_, _ = buf.WriteString(`"fields": [`)
		_, _ = buf.WriteString(strings.Join(fields, ", "))
		_, _ = buf.WriteString(`]`)
		_, _ = buf.WriteString(`}`)
		p = []byte(buf.String())
		break
	case reflect.Ptr:
		elem := rt.Elem()
		if elem.Kind() != reflect.Struct {
			err = fmt.Errorf("%s is not supported, element of ptr must be struct", rt.String())
			return
		}
		p, err = parse(ctx, elem)
		break
	case reflect.Slice:
		if rt.ConvertibleTo(bytesType) {
			_, _ = buf.WriteString(`"bytes"`)
			p = []byte(buf.String())
			break
		}
		_, _ = buf.WriteString(`{`)
		_, _ = buf.WriteString(`"type": "array", `)
		_, _ = buf.WriteString(`"items": `)
		sub, subErr := parse(ctx, rt.Elem())
		if subErr != nil {
			err = subErr
			return
		}
		_, _ = buf.Write(sub)
		_, _ = buf.WriteString(`}`)
		p = []byte(buf.String())
		break
	case reflect.Map:
		if rt.Key().Kind() != reflect.String {
			err = fmt.Errorf("%s is not supported, key of map must be string", rt.String())
			return
		}
		_, _ = buf.WriteString(`{`)
		_, _ = buf.WriteString(`"type": "map", `)
		_, _ = buf.WriteString(`"values": `)
		sub, subErr := parse(ctx, rt.Elem())
		if subErr != nil {
			err = subErr
			return
		}
		_, _ = buf.Write(sub)
		_, _ = buf.WriteString(`}`)
		p = []byte(buf.String())
		break
	default:
		err = fmt.Errorf("%s is not supported", rt.String())
		return
	}
	return
}

const (
	tag = "avro"
)

func parseStructFields(ctx context.Context, rt reflect.Type) (fields []string, err error) {
	num := rt.NumField()
	for i := 0; i < num; i++ {
		ft := rt.Field(i)
		if !ft.IsExported() {
			continue
		}
		name := strings.TrimSpace(ft.Tag.Get(tag))
		if name == "" {
			name = ft.Name
		}
		if ft.Anonymous {
			sub, subErr := parseStructFields(ctx, ft.Type)
			if subErr != nil {
				err = subErr
				return
			}
			fields = append(fields, sub...)
			continue
		}
		p, parseErr := parse(ctx, ft.Type)
		if parseErr != nil {
			err = errors.Join(fmt.Errorf("parse %s field failed", ft.Name), parseErr)
			return
		}
		buf := bytebufferpool.Get()
		_, _ = buf.WriteString(`{`)
		_, _ = buf.WriteString(`"name": "`)
		_, _ = buf.WriteString(name)
		_, _ = buf.WriteString(`", `)
		_, _ = buf.WriteString(`"type": `)
		_, _ = buf.Write(p)
		_, _ = buf.WriteString(`}`)
		fields = append(fields, buf.String())
		bytebufferpool.Put(buf)
	}
	if len(fields) == 0 {
		err = fmt.Errorf("no fields")
		return
	}
	return
}
