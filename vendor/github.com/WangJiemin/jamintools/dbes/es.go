package dbes

import (
	"fmt"
	"time"

	"github.com/juju/errors"

	"gopkg.in/olivere/elastic.v6"
)

type EsAddr struct {
	Host string
	Port int
}

func (this EsAddr) GetUr() string {
	return fmt.Sprintf("http://%s:%d", this.Host, this.Port)
}

type EsConfig struct {
	Addrs               []EsAddr
	User                string
	Password            string
	HealthCheck         bool
	HealthCheckInterval int // second
	HealthCheckTimeout  int // second
	MaxRetries          int
	Sniff               bool
	SniffInterval       int // second
	SniffTimeout        int //second
}

func (this *EsConfig) Check() error {
	if len(this.Addrs) < 1 {
		return errors.Errorf("es addrs is empty")
	}
	return nil
}

func (this *EsConfig) SetDefaultNotOverwrite() {
	if len(this.Addrs) == 0 {
		this.Addrs = []EsAddr{{Host: "127.0.0.1", Port: 9200}}
	}
	if this.HealthCheckInterval <= 0 {
		this.HealthCheckInterval = 15
	}
	if this.HealthCheckTimeout <= 0 {
		this.HealthCheckTimeout = 3
	}
	if this.MaxRetries <= 0 {
		this.MaxRetries = 2
	}
	if this.SniffInterval <= 0 {
		this.SniffInterval = 10
	}
	if this.SniffTimeout <= 0 {
		this.SniffTimeout = 3
	}
}

func (this *EsConfig) GetEsClient() (*elastic.Client, error) {
	var (
		optsArr []elastic.ClientOptionFunc = nil
		urlArr  []string
	)
	for _, oneAddr := range this.Addrs {
		urlArr = append(urlArr, oneAddr.GetUr())
	}

	optsArr = append(optsArr, elastic.SetURL(urlArr...))
	if this.User != "" && this.Password != "" {
		optsArr = append(optsArr, elastic.SetBasicAuth(this.User, this.Password))
	}
	if this.HealthCheck {
		optsArr = append(optsArr, elastic.SetHealthcheck(true))
		if this.HealthCheckInterval > 0 {
			optsArr = append(optsArr, elastic.SetHealthcheckInterval(time.Duration(this.HealthCheckInterval)*time.Second))
		}
		if this.HealthCheckTimeout > 0 {
			optsArr = append(optsArr, elastic.SetHealthcheckTimeout(time.Duration(this.HealthCheckTimeout)*time.Second))
		}
	}

	if this.MaxRetries > 0 {
		optsArr = append(optsArr, elastic.SetMaxRetries(this.MaxRetries))
	}

	if this.Sniff {
		optsArr = append(optsArr, elastic.SetSniff(true))
		if this.SniffInterval > 0 {
			optsArr = append(optsArr, elastic.SetSnifferInterval(time.Duration(this.SniffInterval)*time.Second))
		}
		if this.SniffTimeout > 0 {
			optsArr = append(optsArr, elastic.SetSnifferTimeout(time.Duration(this.SniffTimeout)*time.Second))
		}
	}
	return elastic.NewClient(optsArr...)

}
