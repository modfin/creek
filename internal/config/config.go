package config

import (
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

type Config struct {
	LogLevel       string     `cli:"log-level"`
	Tables         []string   `cli:"tables"`
	PgConfig       PgConfig   `cli-prefix:"pg-"`
	NatsConfig     NatsConfig `cli-prefix:"nats-"`
	PrometheusPort int        `cli:"prometheus-port"`
}

type NatsConfig struct {
	Uri        string          `cli:"uri"`
	Timeout    time.Duration   `cli:"timeout"`
	MaxPending int             `cli:"max-pending"`
	NameSpace  string          `cli:"namespace"`
	Retention  RetentionConfig `cli-prefix:"retention-"`
	Replicas   int             `cli:"replicas"`
}

type RetentionConfig struct {
	Policy   jetstream.RetentionPolicy `cli:"policy"`
	MaxAge   time.Duration             `cli:"max-age"`
	MaxBytes int64                     `cli:"max-bytes"`
	MaxMsgs  int64                     `cli:"max-msgs"`
}

type PgConfig struct {
	Uri             string        `cli:"uri"`
	PublicationName string        `cli:"publication-name"`
	PublicationSlot string        `cli:"publication-slot"`
	MessageTimeout  time.Duration `cli:"message-timeout"`
}
