package config

import (
	"sync"
	"time"

	"github.com/sirupsen/logrus"
)

type Config struct {
	LogLevel string `env:"LOG_LEVEL" envDefault:"info" yaml:"log_level"`

	PgUri             string        `env:"PG_URI,required" yaml:"pg_uri"`
	PgPublicationName string        `env:"PG_PUBLICATION_NAME,required" yaml:"pg_publication_name"`
	PgPublicationSlot string        `env:"PG_PUBLICATION_SLOT,required" yaml:"pg_publication_slot"`
	PgMessageTimeout  time.Duration `env:"PG_MESSAGE_TIMEOUT" envDefault:"10s" yaml:"pg_message_timeout"`
	PgTables          []string      `env:"PG_TABLES,required" envSeparator:" " yaml:"pg_tables"`

	NatsUri        string        `env:"NATS_URI" yaml:"nats_uri"`
	NatsTimeout    time.Duration `env:"NATS_TIMEOUT" envDefault:"30s" yaml:"nats_timeout"`
	NatsMaxPending int           `env:"NATS_MAX_PENDING" envDefault:"4000" yaml:"nats_max_pending"`
	NatsNameSpace  string        `env:"NATS_NAMESPACE" envDefault:"creek" yaml:"nats_namespace"`

	PrometheusPort int `env:"PROMETHEUS_PORT" envDefault:"7779"`
}

var cfg Config
var load sync.Once

func Set(conf Config) {
	cfg = conf
}

func Get() Config {

	load.Do(func() {
		ll, err := logrus.ParseLevel(cfg.LogLevel)
		if err != nil {
			ll = logrus.InfoLevel
		}
		logrus.SetLevel(ll)
	})
	return cfg
}
