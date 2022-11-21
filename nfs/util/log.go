// Copyright © 2017 VMware, Inc. All Rights Reserved.
// SPDX-License-Identifier: BSD-2-Clause
//
// Copyright 2016 VMware, Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package util

import (
	"os"
	"testing"

	"github.com/sirupsen/logrus"
)

var DefaultLogger *logrus.Logger
var Tlog *testing.T


func init() {
	DefaultLogger = &logrus.Logger{
		Out: os.Stderr,
		Formatter: new(logrus.TextFormatter),
		Hooks: make(logrus.LevelHooks),
		Level: logrus.WarnLevel,
	  }
}

func Errorf(format string, args ...interface{}) {
	if Tlog != nil {
		Tlog.Errorf(format, args...)
	} else {
		DefaultLogger.Errorf(format, args...)
	}
}

func Debugf(format string, args ...interface{}) {
	if Tlog != nil {
		Tlog.Logf(format, args...)
	} else {
		DefaultLogger.Debugf(format, args...)
	}
}

func Infof(format string, args ...interface{}) {
	if Tlog != nil {
		Tlog.Logf(format, args...)
	} else {
		DefaultLogger.Infof(format, args...)
	}
}
