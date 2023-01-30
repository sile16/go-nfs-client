package nfs

import (
    "github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/push"
)

var (
    httpRequestsCounter = prometheus.NewCounter(prometheus.CounterOpts{
        Name: "http_requests_total",
        Help: "The total number of HTTP requests",
    })


	metrics_pusher *push.Pusher
)

func metrics_init() {
    prometheus.MustRegister(httpRequestsCounter)

    metrics_pusher = push.New("http://127.0.0.1:9091", "my_job")
	metrics_pusher.Collector(httpRequestsCounter)
}

func metrics_push() {
	metrics_pusher.Push()
}

