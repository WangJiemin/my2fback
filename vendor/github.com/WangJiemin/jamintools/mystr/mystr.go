package mystr

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

func SliceToStringUint32(t []uint32, sep string) string {
	if t == nil || len(t) == 0 {
		return ""
	}
	arr := make([]string, len(t))

	for i, v := range t {
		arr[i] = fmt.Sprintf("%d", v)
	}

	return strings.Join(arr, sep)
}
func SliceToStringUint64(t []uint64, sep string) string {
	arr := make([]string, len(t))

	for i, v := range t {
		arr[i] = fmt.Sprintf("%d", v)
	}

	return strings.Join(arr, sep)
}

func Uin32SliceToStringSlice(t []uint32) []string {
	arr := make([]string, len(t))

	for i, v := range t {
		arr[i] = fmt.Sprintf("%d", v)
	}

	return arr
}

func Uint8SliceToStringSlice(t []uint8) []string {
	arr := make([]string, len(t))

	for i, v := range t {
		arr[i] = fmt.Sprintf("%d", v)
	}

	return arr
}

func UintSliceToStringSlice(t []uint) []string {
	arr := make([]string, len(t))

	for i, v := range t {
		arr[i] = fmt.Sprintf("%d", v)
	}

	return arr
}
func IntSliceToSting(arr []int, sep string) string {
	var strArr []string
	for _, i := range arr {
		strArr = append(strArr, fmt.Sprintf("%d", i))
	}
	return strings.Join(strArr, sep)
}

func IntSliceToStringSlice(intArr []interface{}) []string {
	arrStr := make([]string, len(intArr))
	for j, v := range intArr {
		switch i := v.(type) {
		case int64, int32, int16, int8, int, uint64, uint32, uint16, uint8, uint:
			arrStr[j] = fmt.Sprintf("%d", i)
		default:
			arrStr[j] = fmt.Sprintf("%v", i)
		}
	}
	return arrStr
}

func ParseStringInterfaceToInt64(k string, m map[string]interface{}) int64 {
	var (
		tmpStr string
		tmpInt int64
		ok     bool
		err    error
	)
	_, ok = m[k]
	if !ok {
		return -1
	}
	tmpStr, ok = m[k].(string)
	if !ok {
		return -1
	}
	tmpInt, err = strconv.ParseInt(tmpStr, 10, 64)
	if err != nil {
		return -1
	}
	return tmpInt
}

func ParseStringInterfaceToFloat64(k string, m map[string]interface{}) float64 {
	var (
		tmpStr   string
		tmpFloat float64
		ok       bool
		err      error
	)
	_, ok = m[k]
	if !ok {
		return -1
	}
	tmpStr, ok = m[k].(string)
	if !ok {
		return -1
	}
	tmpFloat, err = strconv.ParseFloat(tmpStr, 64)
	if err != nil {
		return -1
	}
	return tmpFloat
}

func StringToUint32(s string) (uint32, error) {
	u, err := strconv.ParseUint(s, 10, 32)
	if err != nil {
		return 0, err
	} else {
		return uint32(u), nil
	}
}

func StringToUint32Slice(arr []string) ([]uint32, error) {
	var (
		result []uint32
		err    error
		tmp    uint32
	)
	for _, s := range arr {
		tmp, err = StringToUint32(s)
		if err != nil {
			return nil, err
		}
		result = append(result, tmp)
	}
	return result, nil
}

func GetQuotedStringFromArr(arr []string, quote, sep string) string {
	a := make([]string, len(arr))
	for i := range arr {
		a[i] = quote + arr[i] + quote
	}
	return strings.Join(a, sep)
}

func GetPortIntFromStringLines(lines string) ([]int, error) {
	var (
		reg   *regexp.Regexp = regexp.MustCompile(`^\d+$`)
		arr   []string       = strings.Split(lines, "\n")
		ptArr []int
		line  string
		err   error
		pt    uint64
	)
	for _, line = range arr {
		line = strings.TrimSpace(line)
		if !reg.MatchString(line) {
			continue
		}
		pt, err = strconv.ParseUint(line, 10, 32)
		if err == nil {
			ptArr = append(ptArr, int(pt))
		}
	}
	return ptArr, err
}
