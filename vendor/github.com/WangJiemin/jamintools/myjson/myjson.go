package myjson

import (
	"encoding/json"

	kitsFile "github.com/toolkits/file"
	"github.com/WangJiemin/jamintools/ehand"
)

func DumpValueToJsonFile(val interface{}, jFile string) error {
	jBytes, err := json.MarshalIndent(val, "", "\t")
	if err != nil {
		return ehand.WithStackError(err)
	}
	_, err = kitsFile.WriteBytes(jFile, jBytes)
	if err != nil {
		return ehand.WithStackError(err)
	}
	return nil
}

func ReadJsonFileIntoVar(val interface{}, jFile string) error {
	jBytes, err := kitsFile.ToBytes(jFile)
	if err != nil {
		return ehand.WithStackError(err)
	}
	err = json.Unmarshal(jBytes, val)
	if err != nil {
		return ehand.WithStackError(err)
	}

	return nil
}
