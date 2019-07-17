package ehand

import (
	"fmt"
	"os"
	"runtime/debug"
	"strings"

	"github.com/go-errors/errors"
	myLogger "github.com/sirupsen/logrus"
)

func CheckErr(logWr *myLogger.Logger, err *errors.Error, logFields myLogger.Fields, msg string, ifExt bool) {
	if err != nil {
		//logFields["errcode"] = errCode
		errStr := err.Error()
		if errStr != "" {
			errStr += "\n"
		}
		logFields["stacktrace"] = errStr + strings.Replace(err.ErrorStack(), "\n", "\n\t", -1)

		logWr.WithFields(logFields).Error(msg)

		if ifExt {
			var errCode int

			_, ok := logFields["errcode"]
			if ok {
				errCode, ok = logFields["errcode"].(int)
				if !ok {
					errCode = 1
				}
			} else {
				errCode = 1
			}
			logWr.Errorln("Error Exits!")
			os.Exit(errCode)
		}
	}
}

func CheckErrNoExtraMsg(logWr *myLogger.Logger, err *errors.Error, logFields myLogger.Fields, ifExt bool) {
	CheckErr(logWr, err, logFields, "", ifExt)
}

func CheckErrAlreadyStack(logWr *myLogger.Logger, err error, logFields myLogger.Fields, msg string, ifExt bool) {
	if err != nil {
		//logFields["errcode"] = errCode
		errStr := err.Error()
		logFields["stacktrace"] = strings.Replace(errStr, "\n", "\n\t", -1)

		logWr.WithFields(logFields).Error(msg)

		if ifExt {
			var errCode int

			_, ok := logFields["errcode"]
			if ok {
				errCode, ok = logFields["errcode"].(int)
				if !ok {
					errCode = 1
				}
			} else {
				errCode = 1
			}
			logWr.Errorln("Error Exits!")
			os.Exit(errCode)
		}
	}
}

func CheckErrNoExtraMsgAlreadyStack(logWr *myLogger.Logger, err error, logFields myLogger.Fields, ifExt bool) {
	CheckErrAlreadyStack(logWr, err, logFields, "", ifExt)
}

func WithStackError(err error) error {
	serr := errors.Errorf(err.Error())
	return fmt.Errorf(serr.ErrorStack())
}

func CreateErrorWithStack(err error) error {
	return fmt.Errorf("%s\n\t%s", err.Error(), string(debug.Stack()))
}

func CreateStrErrorWithStack(msg string) error {
	return fmt.Errorf("%s\n\t%s", msg, string(debug.Stack()))
}

func CreateMsgWithStack(msg string) string {
	return fmt.Sprintf("%s\n\t%s", msg, string(debug.Stack()))
}

func PanicWithExtraMsg(err error, fields map[string]interface{}, msg string) {
	errStr := ""
	for k, v := range fields {
		errStr = fmt.Sprintf("%s %s=%s", errStr, k, v)
	}
	if msg != "" {
		errStr = fmt.Sprintf("%s extramsg=%s", errStr, msg)
	}
	if err != nil {
		errStr = fmt.Sprintf("%s errdetail=%s", errStr, err)
	}
	panic(errStr)
}

func WriteLogAndPanicAlreadyStack(logWr *myLogger.Logger, err error, logFields myLogger.Fields, extraMsg string) {
	if err != nil {

		logFields["stacktrace"] = strings.Replace(err.Error(), "\n", "\n\t", -1)
		logWr.WithFields(logFields).Error(extraMsg)
		panic(extraMsg)
	}
}

func WriteLogAndPanic(logWr *myLogger.Logger, err *errors.Error, logFields myLogger.Fields, extraMsg string) {
	if err != nil {
		logFields["stacktrace"] = strings.Replace(err.ErrorStack(), "\n", "\n\t", -1)
		logWr.WithFields(logFields).Error(extraMsg)
		panic(extraMsg)
	}
}
