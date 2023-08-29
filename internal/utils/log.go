package utils

import (
	"github.com/sirupsen/logrus"
	"golang.org/x/time/rate"
)

// Inspired by https://github.com/misho-kr/logrus-hooks/blob/master/limit.go

type RateLimitHook struct {
	limiter   *rate.Limiter
	logger    *logrus.Logger
	discarded int
}

func NewRateLimitHook(limitPerSecond float32, burst int, level logrus.Level) *RateLimitHook {
	l := logrus.New()
	l.SetLevel(level)
	return &RateLimitHook{
		limiter: rate.NewLimiter(rate.Limit(limitPerSecond), burst),
		logger:  l,
	}
}

func (hook *RateLimitHook) Fire(entry *logrus.Entry) error {
	if !hook.limiter.Allow() {
		hook.discarded++
		return nil
	}

	hook.logger.WithField("discarded", hook.discarded).Log(entry.Level, entry.Message)
	hook.discarded = 0

	//return errors.New("wow")
	return nil
}

func (hook *RateLimitHook) Levels() []logrus.Level {
	return logrus.AllLevels
}
