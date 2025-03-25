package config

import (
	"time"
)

type Config struct {
	LogLevel string `cli:"log-level"`

	PgUri             string        `cli:"pg-uri"`
	PgPublicationName string        `cli:"pg-publication-name"`
	PgPublicationSlot string        `cli:"pg-publication-slot"`
	PgMessageTimeout  time.Duration `cli:"pg-message-timeout"`
	PgTables          []string      `cli:"tables"`

	NatsUri        string        `cli:"nats-uri"`
	NatsTimeout    time.Duration `cli:"nats-timeout"`
	NatsMaxPending int           `cli:"nats-max-pending"`
	NatsNameSpace  string        `cli:"nats-namespace"`

	PrometheusPort int `cli:"prometheus-port"`
}
