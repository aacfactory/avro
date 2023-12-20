package schemas

import (
	"reflect"
	"time"
)

var (
	bytesType = reflect.TypeOf([]byte{})
	timeType  = reflect.TypeOf(time.Time{})
)
