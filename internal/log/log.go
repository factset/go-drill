package log

import (
	"os"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

var Logger zerolog.Logger

func init() {
	loglevel := os.Getenv("GO_DRILL_LOG_LEVEL")
	lvl, err := zerolog.ParseLevel(loglevel)
	if err != nil {
		log.Printf("invalid value '%s' given for GO_DRILL_LOG_LEVEL. ignoring", loglevel)
	}

	Logger = zerolog.New(zerolog.ConsoleWriter{Out: os.Stderr, TimeFormat: time.RFC822}).Level(lvl).With().Timestamp().Logger()
}

func Printf(format string, v ...interface{}) {
	Logger.Printf(format, v...)
}

func Print(v ...interface{}) {
	Logger.Print(v...)
}
