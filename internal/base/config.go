package base

import (
	"encoding/binary"
	"errors"
	"golang.org/x/sync/singleflight"
	"io"
	"sync"
	"unsafe"

	"github.com/modern-go/reflect2"
)

const maxByteSliceSize = 1024 * 1024

// DefaultConfig is the default API.
var DefaultConfig = Config{}.Freeze()

// Config customises how the codec should behave.
type Config struct {
	// TagKey is the struct tag key used when en/decoding structs.
	// This defaults to "avro".
	TagKey string

	// BlockLength is the length of blocks for maps and arrays.
	// This defaults to 100.
	BlockLength int

	// DisableBlockSizeHeader disables encoding of an array/map size in bytes.
	// Encoded array/map will be prefixed with only the number of elements in
	// contrast with default behavior which prefixes them with the number of elements
	// and the total number of bytes in the array/map. Both approaches are valid according to the
	// Avro specification, however not all decoders support the latter.
	DisableBlockSizeHeader bool

	// UnionResolutionError determines if an error will be returned
	// when a type cannot be resolved while decoding a union.
	UnionResolutionError bool

	// PartialUnionTypeResolution dictates if the union type resolution
	// should be attempted even when not all union types are registered.
	// When enabled, the underlying type will get resolved if it is registered
	// even if other types of the union are not. If resolution fails, logic
	// falls back to default union resolution behavior based on the value of
	// UnionResolutionError.
	PartialUnionTypeResolution bool

	// Disable caching layer for encoders and decoders, forcing them to get rebuilt on every
	// call to Marshal() and Unmarshal()
	DisableCaching bool

	// MaxByteSliceSize is the maximum size of `bytes` or `string` types the Reader will create, defaulting to 1MiB.
	// If this size is exceeded, the Reader returns an error. This can be disabled by setting a negative number.
	MaxByteSliceSize int
}

// Freeze makes the configuration immutable.
func (c Config) Freeze() API {
	api := &frozenConfig{
		config:   c,
		resolver: NewTypeResolver(),
	}

	api.readerPool = &sync.Pool{
		New: func() any {
			return &Reader{
				cfg:    api,
				reader: nil,
				buf:    nil,
				head:   0,
				tail:   0,
			}
		},
	}
	api.writerPool = &sync.Pool{
		New: func() any {
			return &Writer{
				cfg:   api,
				out:   nil,
				buf:   make([]byte, 0, 512),
				Error: nil,
			}
		},
	}

	api.processingGroup = new(singleflight.Group)
	api.processingGroupKeys = &sync.Pool{}

	return api
}

// API represents a frozen Config.
type API interface {
	// Marshal returns the Avro encoding of v.
	Marshal(schema Schema, v any) ([]byte, error)

	// Unmarshal parses the Avro encoded data and stores the result in the value pointed to by v.
	// If v is nil or not a pointer, Unmarshal returns an error.
	Unmarshal(schema Schema, data []byte, v any) error

	// NewEncoder returns a new encoder that writes to w using schema.
	NewEncoder(schema Schema, w io.Writer) *Encoder

	// NewDecoder returns a new decoder that reads from reader r using schema.
	NewDecoder(schema Schema, r io.Reader) *Decoder

	// DecoderOf returns the value decoder for a given schema and type.
	DecoderOf(schema Schema, typ reflect2.Type) ValDecoder

	// EncoderOf returns the value encoder for a given schema and type.
	EncoderOf(schema Schema, tpy reflect2.Type) ValEncoder

	// Register registers names to their types for resolution. All primitive types are pre-registered.
	Register(name string, obj any)
}

type frozenConfig struct {
	config Config

	decoderCache sync.Map // map[cacheKey]ValDecoder
	encoderCache sync.Map // map[cacheKey]ValEncoder

	processingDecoderCache sync.Map // map[cacheKey]ValDecoder
	processingEncoderCache sync.Map // map[cacheKey]ValEncoder

	processingGroup     *singleflight.Group
	processingGroupKeys *sync.Pool

	readerPool *sync.Pool
	writerPool *sync.Pool

	resolver *TypeResolver
}

func (c *frozenConfig) Marshal(schema Schema, v any) ([]byte, error) {
	writer := c.borrowWriter()

	writer.WriteVal(schema, v)
	if err := writer.Error; err != nil {
		c.returnWriter(writer)
		return nil, err
	}

	result := writer.Buffer()
	copied := make([]byte, len(result))
	copy(copied, result)

	c.returnWriter(writer)
	return copied, nil
}

func (c *frozenConfig) borrowWriter() *Writer {
	writer := c.writerPool.Get().(*Writer)
	writer.Reset(nil)
	return writer
}

func (c *frozenConfig) returnWriter(writer *Writer) {
	writer.out = nil
	writer.Error = nil

	c.writerPool.Put(writer)
}

func (c *frozenConfig) Unmarshal(schema Schema, data []byte, v any) error {
	reader := c.borrowReader(data)

	reader.ReadVal(schema, v)
	err := reader.Error
	c.returnReader(reader)

	if errors.Is(err, io.EOF) {
		return nil
	}

	return err
}

func (c *frozenConfig) borrowReader(data []byte) *Reader {
	reader := c.readerPool.Get().(*Reader)
	reader.Reset(data)
	return reader
}

func (c *frozenConfig) returnReader(reader *Reader) {
	reader.Error = nil
	c.readerPool.Put(reader)
}

func (c *frozenConfig) NewEncoder(schema Schema, w io.Writer) *Encoder {
	writer, ok := w.(*Writer)
	if !ok {
		writer = NewWriter(w, 512, WithWriterConfig(c))
	}
	return &Encoder{
		s: schema,
		w: writer,
	}
}

func (c *frozenConfig) NewDecoder(schema Schema, r io.Reader) *Decoder {
	reader := NewReader(r, 512, WithReaderConfig(c))
	return &Decoder{
		s: schema,
		r: reader,
	}
}

func (c *frozenConfig) Register(name string, obj any) {
	c.resolver.Register(name, obj)
}

type cacheKey struct {
	fingerprint [32]byte
	rtype       uintptr
}

func (c *frozenConfig) addDecoderToCache(fingerprint [32]byte, rtype uintptr, dec ValDecoder) {
	if c.config.DisableCaching {
		return
	}
	key := cacheKey{fingerprint: fingerprint, rtype: rtype}
	c.decoderCache.Store(key, dec)
}

func (c *frozenConfig) getDecoderFromCache(fingerprint [32]byte, rtype uintptr) ValDecoder {
	if c.config.DisableCaching {
		return nil
	}
	key := cacheKey{fingerprint: fingerprint, rtype: rtype}
	if dec, ok := c.decoderCache.Load(key); ok {
		return dec.(ValDecoder)
	}

	return nil
}

func (c *frozenConfig) addEncoderToCache(fingerprint [32]byte, rtype uintptr, enc ValEncoder) {
	if c.config.DisableCaching {
		return
	}
	key := cacheKey{fingerprint: fingerprint, rtype: rtype}
	c.encoderCache.Store(key, enc)
}

func (c *frozenConfig) getEncoderFromCache(fingerprint [32]byte, rtype uintptr) ValEncoder {
	if c.config.DisableCaching {
		return nil
	}
	key := cacheKey{fingerprint: fingerprint, rtype: rtype}
	if enc, ok := c.encoderCache.Load(key); ok {
		return enc.(ValEncoder)
	}

	return nil
}

func (c *frozenConfig) addProcessingDecoderToCache(fingerprint [32]byte, rtype uintptr, dec ValDecoder) {
	key := cacheKey{fingerprint: fingerprint, rtype: rtype}
	c.processingDecoderCache.Store(key, dec)
}

func (c *frozenConfig) getProcessingDecoderFromCache(fingerprint [32]byte, rtype uintptr) ValDecoder {
	key := cacheKey{fingerprint: fingerprint, rtype: rtype}
	if !c.config.DisableCaching {
		if dec, ok := c.decoderCache.Load(key); ok {
			return dec.(ValDecoder)
		}
	}
	if dec, ok := c.processingDecoderCache.Load(key); ok {
		return dec.(ValDecoder)
	}
	return nil
}

func (c *frozenConfig) addProcessingEncoderToCache(fingerprint [32]byte, rtype uintptr, enc ValEncoder) {
	key := cacheKey{fingerprint: fingerprint, rtype: rtype}
	c.processingEncoderCache.Store(key, enc)
}

func (c *frozenConfig) getProcessingEncoderFromCache(fingerprint [32]byte, rtype uintptr) ValEncoder {
	key := cacheKey{fingerprint: fingerprint, rtype: rtype}
	if !c.config.DisableCaching {
		if enc, ok := c.encoderCache.Load(key); ok {
			return enc.(ValEncoder)
		}
	}
	if enc, ok := c.processingEncoderCache.Load(key); ok {
		return enc.(ValEncoder)
	}
	return nil
}

var (
	encoderProcessingKeyType = []byte{1}
	decoderProcessingKeyType = []byte{2}
)

func (c *frozenConfig) borrowProcessEncoderGroupKey(schema Schema, typ reflect2.Type) (key []byte) {
	k := c.processingGroupKeys.Get()
	if k != nil {
		key = *(k.(*[]byte))
	} else {
		key = make([]byte, 64)
	}
	fingerprint := schema.Fingerprint()
	copy(key[:32], fingerprint[:])
	binary.LittleEndian.PutUint64(key[32:], uint64(typ.RType()))
	copy(key[63:], encoderProcessingKeyType)
	return
}

func (c *frozenConfig) borrowProcessDecoderGroupKey(schema Schema, typ reflect2.Type) (key []byte) {
	k := c.processingGroupKeys.Get()
	if k != nil {
		key = *(k.(*[]byte))
	} else {
		key = make([]byte, 64)
	}
	fingerprint := schema.Fingerprint()
	copy(key[:32], fingerprint[:])
	binary.LittleEndian.PutUint64(key[32:], uint64(typ.RType()))
	copy(key[63:], decoderProcessingKeyType)
	return
}

func (c *frozenConfig) returnProcessGroupKey(key []byte) {
	c.processingGroup.Forget(unsafe.String(unsafe.SliceData(key), len(key)))
	c.processingGroupKeys.Put(&key)
}

func (c *frozenConfig) getTagKey() string {
	tagKey := c.config.TagKey
	if tagKey == "" {
		return "avro"
	}
	return tagKey
}

func (c *frozenConfig) getBlockLength() int {
	blockSize := c.config.BlockLength
	if blockSize <= 0 {
		return 100
	}
	return blockSize
}

func (c *frozenConfig) getMaxByteSliceSize() int {
	size := c.config.MaxByteSliceSize
	if size == 0 {
		return maxByteSliceSize
	}
	return size
}
