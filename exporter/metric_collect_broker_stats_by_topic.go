package exporter

import (
	"context"
	"strings"

	"github.com/apache/rocketmq-client-go/v2/rlog"
	"github.com/prometheus/client_golang/prometheus"
)

const (
	TopicPutNums    = "TOPIC_PUT_NUMS"
	TopicPutSize    = "TOPIC_PUT_SIZE"
	GroupGetNums    = "GROUP_GET_NUMS"
	GroupGetSize    = "GROUP_GET_SIZE"
	SendbackPutNums = "SNDBCK_PUT_NUMS"
)

func (e *RocketmqExporter) CollectBrokerStatsByTopic(ch chan<- prometheus.Metric, topic string) {

	if strings.HasPrefix(topic, DlqGroupTopicPrefix) || strings.HasPrefix(topic, RetryGroupTopicPrefix) {
		return
	}

	for _, broker := range e.getRouteInfoByTopic(topic).BrokerDataList {

		var brokerAddress = broker.SelectBrokerAddr()

		topicPuNumBrokerStats, err := e.admin.QueryBrokerStats(context.Background(), TopicPutNums, topic, brokerAddress)

		if err != nil {
			rlog.Error("CollectBrokerStatsByTopic QueryBrokerStats ", map[string]interface{}{
				"statsName": TopicPutNums,
				"statsKey":  topic,
				"broker":    broker.BrokerName,
				"err":       err,
			})
		}
		if topicPuNumBrokerStats != nil {
			ch <- prometheus.MustNewConstMetric(
				rocketmqProducerTps,
				prometheus.GaugeValue,
				topicPuNumBrokerStats.StatsMinute.Tps,
				broker.Cluster,
				broker.BrokerName,
				topic,
			)
		}

		topicPutSizeBrokerStats, err := e.admin.QueryBrokerStats(context.Background(), TopicPutSize, topic, brokerAddress)
		if err != nil {
			rlog.Error("CollectBrokerStatsByTopic QueryBrokerStats ", map[string]interface{}{
				"statsName": TopicPutSize,
				"statsKey":  topic,
				"broker":    broker.BrokerName,
				"err":       err,
			})
		}
		if topicPutSizeBrokerStats != nil {
			ch <- prometheus.MustNewConstMetric(
				rocketmqProducerMessageSize,
				prometheus.GaugeValue,
				topicPutSizeBrokerStats.StatsMinute.Tps,
				broker.Cluster,
				broker.BrokerName,
				topic,
			)
		}

	}

	groups, err := e.admin.ExamineTopicConsumeByWho(context.Background(), topic)

	if err != nil {
		rlog.Error("CollectBrokerStatsByTopic ExamineTopicConsumeByWho:", map[string]interface{}{
			"topic": topic,
			"err":   err,
		})
		return
	}

	for _, group := range groups {

		for _, broker := range e.getRouteInfoByTopic(topic).BrokerDataList {
			var brokerAddress = broker.SelectBrokerAddr()
			if brokerAddress != "" {
				var statsKey = topic + "@" + group

				groupGetNumBrokerStats, err := e.admin.QueryBrokerStats(context.Background(), GroupGetNums, statsKey, brokerAddress)
				if err != nil {
					rlog.Error("CollectBrokerStatsByTopic QueryBrokerStats ", map[string]interface{}{
						"statsName": GroupGetNums,
						"statsKey":  statsKey,
						"broker":    broker.BrokerName,
						"err":       err,
					})
				}

				if groupGetNumBrokerStats != nil {
					ch <- prometheus.MustNewConstMetric(
						rocketmqConsumerTps,
						prometheus.GaugeValue,
						groupGetNumBrokerStats.StatsMinute.Tps,
						broker.Cluster,
						broker.BrokerName,
						topic,
						group,
					)
				}

				groupGetSizeBrokerStats, err := e.admin.QueryBrokerStats(context.Background(), GroupGetSize, statsKey, brokerAddress)
				if err != nil {
					rlog.Error("CollectBrokerStatsByTopic QueryBrokerStats ", map[string]interface{}{
						"statsName": GroupGetSize,
						"statsKey":  statsKey,
						"broker":    broker.BrokerName,
						"err":       err,
					})
				}
				if groupGetSizeBrokerStats != nil {
					ch <- prometheus.MustNewConstMetric(
						rocketmqConsumerMessageSize,
						prometheus.GaugeValue,
						groupGetSizeBrokerStats.StatsMinute.Tps,
						broker.Cluster,
						broker.BrokerName,
						topic,
						group,
					)
				}

				sendbackPutNumsBrokerStats, err := e.admin.QueryBrokerStats(context.Background(), SendbackPutNums, statsKey, brokerAddress)
				if err != nil {
					rlog.Error("CollectBrokerStatsByTopic QueryBrokerStats ", map[string]interface{}{
						"statsName": SendbackPutNums,
						"statsKey":  statsKey,
						"broker":    broker.BrokerName,
						"err":       err,
					})
				}
				if sendbackPutNumsBrokerStats != nil {
					ch <- prometheus.MustNewConstMetric(
						rocketmqSendBackNums,
						prometheus.GaugeValue,
						float64(sendbackPutNumsBrokerStats.StatsMinute.Sum),
						broker.Cluster,
						broker.BrokerName,
						topic,
						group,
					)
				}
			}
		}

	}
}
