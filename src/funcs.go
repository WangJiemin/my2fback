package src

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/WangJiemin/jamintools/ehand"
	"github.com/WangJiemin/jamintools/logging"
	MyPos "github.com/siddontang/go-mysql/mysql"
	"github.com/toolkits/slice"
)

/*
func CheckErr(err error, errMsg string, errCode int, ifExt bool) {
	if err != nil {
		if errMsg != "" {
			fmt.Printf("%s: %s\n", errMsg, err)
		} else {
			fmt.Println(err)
		}
		if ifExt {
			if errCode < 1 {
				errCode = 1
			}
			os.Exit(errCode)
		}
	}
}
*/

func IntSliceToString(iArr []int, sep string, prefix string) string {
	sArr := make([]string, len(iArr))
	for _, v := range iArr {
		sArr = append(sArr, string(v))
	}

	return prefix + " " + strings.Join(sArr, sep)
}

func StrSliceToString(sArr []string, sep, prefix string) string {
	return prefix + " " + strings.Join(sArr, sep)
}

func CheckElementOfSliceStr(arr []string, element string, prefix string, ifExt bool) bool {
	if slice.ContainsString(arr, element) {
		return true
	} else {
		if ifExt {
			GLogger.WriteToLogByFieldsExitMsgNoErr(fmt.Sprintf("%s, %s",
				prefix, StrSliceToString(arr, ",", "valid args are: ")), logging.ERROR, ehand.ERR_INVALID_OPTION)
		}
		return false
	}
}

func CheckElementOfSliceInt(arr []int, element int, prefix string, ifExt bool) bool {
	if slice.ContainsInt(arr, element) {
		return true
	} else {
		if ifExt {
			GLogger.WriteToLogByFieldsExitMsgNoErr(fmt.Sprintf("%s, %s", prefix,
				IntSliceToString(arr, ",", "valid args are: ")), logging.ERROR, ehand.ERR_INVALID_OPTION)
		}
		return false
	}
}

func CompareBinlogPos(sBinFile string, sPos uint, eBinFile string, ePos uint) int {
	// 1: greater, -1: less, 0: equal
	sp := MyPos.Position{Name: sBinFile, Pos: uint32(sPos)}
	ep := MyPos.Position{Name: eBinFile, Pos: uint32(ePos)}

	result := sp.Compare(ep)

	return result
}

func CheckIsDir(fd string) (bool, string) {
	fs, err := os.Stat(fd)
	if err != nil {
		return false, fd + " not exists"
	}
	if fs.IsDir() {
		return true, ""
	} else {
		return false, fd + " is not a dir"
	}
}

func GetBinlogBasenameAndIndex(binlog string) (string, int) {
	binlogFile := filepath.Base(binlog)
	arr := strings.Split(binlogFile, ".")
	cnt := len(arr)
	n, err := strconv.ParseUint(arr[cnt-1], 10, 32)
	GLogger.WriteToLogByFieldsErrorExtramsgExit(err, "parse binlog file index number error", logging.ERROR, ehand.ERR_NUMBER_PARSE)
	indx := int(n)
	baseName := strings.Join(arr[0:cnt-1], "")
	return baseName, indx
}

func GetNextBinlog(baseName string, indx int) string {
	indx++
	//idxStr := strconv.Itoa(indx)
	idxStr := fmt.Sprintf("%06d", indx)
	return baseName + "." + idxStr
}

func GetDatetimeStr(sec int64, nsec int64, timeFmt string) string {
	return time.Unix(sec, nsec).Format(timeFmt)
}

func CommaSeparatedListToArray(str string) []string {
	var arr []string

	for _, item := range strings.Split(str, ",") {
		item = strings.TrimSpace(item)

		if item != "" {
			arr = append(arr, item)
		}
	}

	return arr
}

func GetAbsTableName(schema, table string) string {
	return fmt.Sprintf("%s%s%s", schema, KEY_DB_TABLE_SEP, table)
}

func GetDbTbFromAbsTbName(name string) (string, string) {
	arr := strings.Split(name, KEY_DB_TABLE_SEP)
	return arr[0], arr[1]
}

func GetBinlogPosAsKey(binlog string, spos, epos uint32) string {
	arr := []string{binlog, strconv.FormatUint(uint64(spos), 10), strconv.FormatUint(uint64(epos), 10)}
	return strings.Join(arr, KEY_BINLOG_POS_SEP)
}

func GetMaxValue(nums ...int) int {
	max := nums[0]
	for _, v := range nums {
		if v > max {
			max = v
		}
	}
	return max
}

func GetMinValue(nums ...int) int {
	min := nums[0]
	for _, v := range nums {
		if v < min {
			min = v
		}
	}
	return min
}

func GetLineHeaderStrFromColumnNamesArr(arr []string, sep string) string {
	return strings.Join(arr, sep)
}

func ConvertStrArrToIntferfaceArrForPrint(arr []string) []interface{} {
	tmp := make([]interface{}, len(arr))
	for i, v := range arr {
		tmp[i] = v
	}
	return tmp
}

func CompareEquelByteSlice(s1 []byte, s2 []byte) bool {
	if len(s1) != len(s2) {
		return false
	}
	for i, v := range s1 {
		if v != s2[i] {
			return false
		}
	}
	return true
}
