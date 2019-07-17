package dsql

import (
	"fmt"
	"regexp"
)

const (
	CgetsrcBinDdl uint8 = 1
	CgetsrcMaster uint8 = 0
)

func GetFullTableName(ifBackQuote bool, db, tb string) string {
	if ifBackQuote {
		return fmt.Sprintf("`%s`.`%s`", db, tb)
	} else {
		return fmt.Sprintf("%s.%s", db, tb)
	}
}

func CheckRegNotEmpty(reg *regexp.Regexp) bool {
	if reg != nil && reg.String() != "" {
		return true
	} else {
		return false
	}
}
