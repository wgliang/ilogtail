// Copyright 2021 iLogtail Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package cls

import (
	"fmt"
	"strings"

	"github.com/alibaba/ilogtail"
	"github.com/alibaba/ilogtail/helper"
	// "github.com/alibaba/ilogtail/pkg/logtail"
	"github.com/alibaba/ilogtail/pkg/protocol"
	"github.com/alibaba/ilogtail/pkg/util"

	"github.com/pierrec/lz4"
	"github.com/tencentcloud/tencentcloud-sdk-go/tencentcloud/common"
	tchttp "github.com/tencentcloud/tencentcloud-sdk-go/tencentcloud/common/http"
	"github.com/tencentcloud/tencentcloud-sdk-go/tencentcloud/common/profile"
	// "go.uber.org/zap"
	cls "github.com/alibaba/ilogtail/plugins/flusher/cls/proto"
	pb "google.golang.org/protobuf/proto"
)

// ClsFlusher uses symbols in LogtailAdaptor(.so) to flush log groups to Logtail.
type ClsFlusher struct {
	EnableShardHash bool
	KeepShardHash   bool
	SecretID        string
	SecretKey       string
	Region          string
	LogSet          string
	Topic           string

	context    ilogtail.Context
	lenCounter ilogtail.CounterMetric
}

// Init ...
func (p *ClsFlusher) Init(context ilogtail.Context) error {
	p.context = context
	p.lenCounter = helper.NewCounterMetric("flush_sls_size")
	return nil
}

// Description ...
func (p *ClsFlusher) Description() string {
	return "logtail flusher for log service"
}

// IsReady ...
// There is a sending queue in Logtail, call LogtailIsValidToSend through cgo
// to make sure if there is any space for coming data.
func (p *ClsFlusher) IsReady(projectName string, logstoreName string, logstoreKey int64) bool {
	return true
}

// Flush ...
// Because IsReady is called before, Logtail must have space in sending queue,
// just call LogtailSendPb through cgo to push data into queue, Logtail will
// send data to its destination (SLS mostly) according to its config.
func (p *ClsFlusher) Flush(projectName string, logstoreName string, configName string, logGroupList []*protocol.LogGroup) error {
	var errstrings []string

	for _, logGroup := range logGroupList {
		if len(logGroup.Logs) == 0 {
			continue
		}

		var shardHash string
		if p.EnableShardHash {
			for idx, tag := range logGroup.LogTags {
				if tag.Key == util.ShardHashTagKey {
					shardHash = tag.Value
					if !p.KeepShardHash {
						logGroup.LogTags = append(logGroup.LogTags[0:idx], logGroup.LogTags[idx+1:]...)
					}
					break
				}
			}
		}

		credential := common.NewCredential(p.SecretID, p.SecretKey)

		c := common.NewCommonClient(credential, p.Region, profile.NewClientProfile())

		headers := map[string]string{
			"X-CLS-TopicId": p.Topic,
			"X-CLS-HashKey": shardHash,
		}
		commpresstype := ""

		logGroup := covertPB(logGroup)

		logGroupList := cls.LogGroupList{
			LogGroupList: []*cls.LogGroup{
				&logGroup,
			},
		}
		data, _ := pb.Marshal(&logGroupList)

		length := lz4.CompressBlockBound(len(data)) + 1
		compressbody := make([]byte, length)
		n, err := lz4.CompressBlock(data, compressbody, nil)
		if err == nil && n > 0 {
			commpresstype = "lz4"
			data = compressbody[0:n]
		}
		headers["X-CLS-CompressType"] = commpresstype

		request := tchttp.NewCommonRequest("cls", "2020-10-16", "UploadLog")
		request.SetOctetStreamParameters(headers, data)

		response := tchttp.NewCommonResponse()

		err = c.SendOctetStream(request, response)
		errstrings = append(errstrings, err.Error())
	}

	if len(errstrings) == 0 {
		return nil
	}

	return fmt.Errorf(strings.Join(errstrings, "\n"))
}

// SetUrgent ...
// We do nothing here because necessary flag has already been set in Logtail
//   before this method is called. Any future call of IsReady will return
//   true so that remainding data can be flushed to Logtail (which will flush
//   data to local file system) before it quits.
func (*ClsFlusher) SetUrgent(flag bool) {
}

// Stop ...
// We do nothing here because ClsFlusher does not create any resources.
func (*ClsFlusher) Stop() error {
	return nil
}

func covertPB(logGroup *protocol.LogGroup) cls.LogGroup {
	logs := make([]*cls.Log, 0)
	for _, log := range logGroup.Logs {
		if log == nil {
			continue
		}
		contents := make([]*cls.Log_Content, 0)
		for _, content := range log.Contents {
			if content == nil || content.Key == "" || content.Value == "" {
				continue
			}

			contents = append(contents, &cls.Log_Content{
				Key:   &content.Key,
				Value: &content.Value,
			})
		}
		time := int64(log.Time)
		logs = append(logs, &cls.Log{
			Time:     &time,
			Contents: contents,
		})
	}

	logTags := make([]*cls.LogTag, 0)
	for _, tag := range logGroup.LogTags {
		if tag == nil || tag.Key == "" || tag.Value == "" {
			continue
		}
		key := strings.Trim(tag.Key, "_")
		value := strings.Trim(tag.Value, "_")
		logTags = append(logTags, &cls.LogTag{
			Key:   &key,
			Value: &value,
		})
	}

	clsLogGroup := cls.LogGroup{
		Logs:        logs,
		ContextFlow: &logGroup.Topic,
		Source:      &logGroup.Source,
		LogTags:     logTags,
	}

	return clsLogGroup
}

func init() {
	ilogtail.Flushers["flusher_cls"] = func() ilogtail.Flusher {
		return &ClsFlusher{
			EnableShardHash: false,
			KeepShardHash:   true,
		}
	}
}
