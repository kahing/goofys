// Copyright 2015 - 2017 Ka-Hing Cheung
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package common

import (
	"fmt"
	glog "log"
	"log/syslog"
	"os"
	"strings"
	"sync"

	"github.com/sirupsen/logrus"
	logrus_syslog "github.com/sirupsen/logrus/hooks/syslog"
)

var mu sync.Mutex
var loggers = make(map[string]*LogHandle)

var log = GetLogger("main")
var cloudLogLevel = logrus.InfoLevel

var syslogHook *logrus_syslog.SyslogHook

func InitLoggers(logToSyslog bool) {
	if logToSyslog {
		var err error
		syslogHook, err = logrus_syslog.NewSyslogHook("", "", syslog.LOG_DEBUG, "")
		if err != nil {
			// we are the child process and we cannot connect to syslog,
			// probably because we are in a container without syslog
			// nothing much we can do here, printing to stderr doesn't work
			return
		}

		for _, l := range loggers {
			l.Hooks.Add(syslogHook)
		}
	}
}

func SetCloudLogLevel(level logrus.Level) {
	cloudLogLevel = level

	for k, logr := range loggers {
		if k != "main" && k != "fuse" {
			logr.Level = level
		}
	}
}

type LogHandle struct {
	logrus.Logger

	name string
	Lvl  *logrus.Level
}

func (l *LogHandle) Format(e *logrus.Entry) ([]byte, error) {
	// Mon Jan 2 15:04:05 -0700 MST 2006
	timestamp := ""
	lvl := e.Level
	if l.Lvl != nil {
		lvl = *l.Lvl
	}

	if syslogHook == nil {
		const timeFormat = "2006/01/02 15:04:05.000000"

		timestamp = e.Time.Format(timeFormat) + " "
	}

	str := fmt.Sprintf("%v%v.%v %v",
		timestamp,
		l.name,
		strings.ToUpper(lvl.String()),
		e.Message)

	if len(e.Data) != 0 {
		str += " " + fmt.Sprint(e.Data)
	}

	str += "\n"
	return []byte(str), nil
}

// for aws.Logger
func (l *LogHandle) Log(args ...interface{}) {
	l.Debugln(args...)
}

func NewLogger(name string) *LogHandle {
	l := &LogHandle{name: name}
	l.Out = os.Stderr
	l.Formatter = l
	l.Level = logrus.InfoLevel
	l.Hooks = make(logrus.LevelHooks)
	if syslogHook != nil {
		l.Hooks.Add(syslogHook)
	}
	return l
}

func GetLogger(name string) *LogHandle {
	mu.Lock()
	defer mu.Unlock()

	if logger, ok := loggers[name]; ok {
		if name != "main" && name != "fuse" {
			logger.SetLevel(cloudLogLevel)
		}
		return logger
	} else {
		logger := NewLogger(name)
		loggers[name] = logger
		if name != "main" && name != "fuse" {
			logger.SetLevel(cloudLogLevel)
		}
		return logger
	}
}

func GetStdLogger(l *LogHandle, lvl logrus.Level) *glog.Logger {
	return glog.New(l.WriterLevel(lvl), "", 0)
}

// retryablehttp logs messages using Printf("[DEBUG|ERR] message")
// logrus.Logger's Printf maps to INFO level, so this logger parses
// the the message to map to the correct log level

type RetryHTTPLogger struct {
	*LogHandle
}

const DEBUG_TAG = "[DEBUG]"
const ERR_TAG = "[ERR]"

func (logger RetryHTTPLogger) Printf(format string, args ...interface{}) {
	// unfortunately logrus.ParseLevel uses "error" instead of "err"
	// so we have to map this ourselves
	if strings.HasPrefix(format, DEBUG_TAG) {
		logger.LogHandle.Debugf(format[len(DEBUG_TAG)+1:], args...)
	} else if strings.HasPrefix(format, ERR_TAG) {
		logger.LogHandle.Errorf(format[len(ERR_TAG)+1:], args...)
	} else {
		logger.LogHandle.Infof(format, args...)
	}

}
