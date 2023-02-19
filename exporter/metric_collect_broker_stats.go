package exporter

import (
	"context"

	"github.com/rocketmq-exporter-go/admin"

	"github.com/apache/rocketmq-client-go/v2/rlog"
	"github.com/prometheus/client_golang/prometheus"
)

const (
	BrokerPutNums = "BROKER_PUT_NUMS"
	BrokerGetNums = "BROKER_GET_NUMS"
)

func (e *RocketmqExporter) CollectBrokerStats(ch chan<- prometheus.Metric, broker *admin.BrokerData) {

	var brokerAddress = broker.SelectBrokerAddr()

	topicPuNumBrokerStats, err := e.admin.QueryBrokerStats(context.Background(), BrokerPutNums, broker.Cluster, brokerAddress)
	if err != nil {
		rlog.Error("CollectBrokerStats QueryBrokerStats ", map[string]interface{}{
			"statsName": BrokerPutNums,
			"statsKey":  broker.Cluster,
			"broker":    broker.BrokerName,
			"err":       err,
		})
	}
	if topicPuNumBrokerStats != nil {
		ch <- prometheus.MustNewConstMetric(
			rocketmqBrokerTps,
			prometheus.GaugeValue,
			float64(topicPuNumBrokerStats.StatsMinute.Sum),
			broker.Cluster,
			broker.BrokerName,
			brokerAddress,
		)
	}

	topicGetNumBrokerStats, err := e.admin.QueryBrokerStats(context.Background(), BrokerGetNums, broker.Cluster, brokerAddress)
	if err != nil {
		rlog.Error("CollectBrokerStats QueryBrokerStats ", map[string]interface{}{
			"statsName": BrokerGetNums,
			"statsKey":  broker.Cluster,
			"broker":    broker.BrokerName,
			"err":       err,
		})
	}

	if topicGetNumBrokerStats != nil {
		ch <- prometheus.MustNewConstMetric(
			rocketmqBrokerQps,
			prometheus.GaugeValue,
			topicGetNumBrokerStats.StatsMinute.Tps,
			broker.Cluster,
			broker.BrokerName,
			brokerAddress,
		)
	}

}
