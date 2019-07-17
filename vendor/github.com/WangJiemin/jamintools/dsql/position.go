package dsql

import (
	"encoding/json"
	"sync"
	"time"

	"github.com/juju/errors"
	gmysql "github.com/siddontang/go-mysql/mysql"
	"github.com/toolkits/file"
	"github.com/WangJiemin/jamintools/myjson"
)

type PosSynced struct {
	Position      gmysql.Position
	Lock          sync.RWMutex
	LastSavedTime time.Time
	SlaveInterval int64
	File          string
	SecondsLag    int64
}

func (this *PosSynced) CheckPos() bool {
	if this.Position.Name == "" || this.Position.Pos <= 0 {
		return false
	} else {
		return true
	}
}
func (this *PosSynced) GetPositionJsonStr() (string, error) {
	this.Lock.RLock()
	defer this.Lock.RUnlock()
	jbytes, err := json.Marshal(this.Position)
	if err != nil {
		return "", errors.Annotatef(err, "error to get synced position json string")
	} else {
		return string(jbytes), nil
	}
}

func (this *PosSynced) SetPosition(p gmysql.Position, lag int64) {
	this.Lock.Lock()
	defer this.Lock.Unlock()
	this.Position.Name = p.Name
	this.Position.Pos = p.Pos
	this.SecondsLag = lag
}

func (this *PosSynced) GetPosition() gmysql.Position {
	this.Lock.RLock()
	defer this.Lock.RUnlock()
	return gmysql.Position{Name: this.Position.Name, Pos: this.Position.Pos}
}

func (this *PosSynced) LoadFromFile() error {

	if !file.IsFile(this.File) {
		return errors.Errorf("%s not exists nor a file", this.File)
	}
	pos := &gmysql.Position{}
	err := myjson.ReadJsonFileIntoVar(pos, this.File)
	if err != nil {
		return errors.Annotatef(err, "error to unmarshal synced position json from file %s", this.File)
	}
	if pos.Name == "" || pos.Pos == 0 {
		return errors.Errorf("position %s from %s is invalid", pos.String(), this.File)
	}
	this.Lock.Lock()
	defer this.Lock.Unlock()
	this.Position.Name = pos.Name
	this.Position.Pos = pos.Pos
	return nil
}

func (this *PosSynced) SaveToFile(ifForce bool) error {
	now := time.Now()
	if !ifForce {
		if (now.Unix() - this.LastSavedTime.Unix()) < this.SlaveInterval {
			return nil
		}
	}
	this.Lock.RLock()
	defer this.Lock.RUnlock()
	err := myjson.DumpValueToJsonFile(this.Position, this.File)
	if err != nil {
		return errors.Annotatef(err, "error to save synced position to %s", this.File)
	}
	this.LastSavedTime = now
	return nil
}
