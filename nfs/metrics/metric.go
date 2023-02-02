package metrics

import (
	"fmt"
	"net"
	"time"

	"github.com/bastjan/netstat"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/push"
)

var (
    RpcRequestsCounter = prometheus.NewCounter(prometheus.CounterOpts{
        Name: "rpc_requests_total",
        Help: "The total number of rpc requests",
    })
    RpcWriteRequestsCounter = prometheus.NewCounter(prometheus.CounterOpts{
        Name: "rpc_write_requests_total",
        Help: "The total number of write rpc requests",
    })
    RpcReadRequestsCounter = prometheus.NewCounter(prometheus.CounterOpts{
        Name: "rpc_read_requests_total",
        Help: "The total number of read rpc requests",
    })
    RpcBytesReadCounter = prometheus.NewCounter(prometheus.CounterOpts{
        Name: "rpc_bytes_read_total",
        Help: "The total number of bytes read",
    })
    RpcBytesWrittenCounter = prometheus.NewCounter(prometheus.CounterOpts{
        Name: "rpc_bytes_written_total",
        Help: "The total number of bytes written",
    })
    RpcOutstandingRequests = prometheus.NewGauge(prometheus.GaugeOpts{
        Name: "rpc_outstanding_requests",
        Help: "The total number of outstanding rpc requests",
    })
    RpcRequestLatencyuS  = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "rpc_latency_us",
		Help:    "Histogram of rpc latency in us", // Sorry, we can't measure how badly it smells.
		//Buckets: prometheus.LinearBuckets(500, 5, 5),  // 5 buckets, each 5 centigrade wide.
        Buckets: prometheus.ExponentialBuckets(500, 2, 6),  // 10 buckets, each 2x wider.
	})
    TcpSendQueue = prometheus.NewGaugeVec(prometheus.GaugeOpts{
        Name: "tcp_send_queue",
        Help: "The total number of outstanding rpc requests",
    }, []string{"remote_addr", "remote_port"})

    TcpRecvQueue = prometheus.NewGaugeVec(prometheus.GaugeOpts{
        Name: "tcp_recv_queue",
        Help: "The total number of outstanding rpc requests",
    }, []string{"remote_addr", "remote_port"})

	metrics_pusher *push.Pusher

    

    //a thread safe list []*net.TCPaddr] to be monitored
    Monitored_addrs = make([]*net.TCPAddr ,0)

)

func Metrics_init(job_name string, push_interval int) {
    
    prometheus.MustRegister(RpcRequestsCounter)
    prometheus.MustRegister(RpcWriteRequestsCounter)
    prometheus.MustRegister(RpcReadRequestsCounter)
    prometheus.MustRegister(RpcOutstandingRequests)
    prometheus.MustRegister(RpcRequestLatencyuS)
    prometheus.MustRegister(TcpSendQueue)
    prometheus.MustRegister(TcpRecvQueue)
    prometheus.MustRegister(RpcBytesReadCounter)
    prometheus.MustRegister(RpcBytesWrittenCounter)

    metrics_pusher = push.New("http://127.0.0.1:9091", job_name)
    metrics_pusher.Collector(RpcRequestsCounter)
    metrics_pusher.Collector(RpcWriteRequestsCounter)
    metrics_pusher.Collector(RpcReadRequestsCounter)
    metrics_pusher.Collector(RpcOutstandingRequests)
    metrics_pusher.Collector(RpcRequestLatencyuS)
    metrics_pusher.Collector(TcpSendQueue)
    metrics_pusher.Collector(TcpRecvQueue)
    metrics_pusher.Collector(RpcBytesReadCounter)
    metrics_pusher.Collector(RpcBytesWrittenCounter)

    RpcOutstandingRequests.Set(0)


    
    go metrics_collect_loop(push_interval)
    go Metrics_push(push_interval)
}

func Metrics_push(push_interval int) {
    //defer RpcOutstandingRequests.Set(0)
    // one last push to capture ending metrics.
    defer metrics_pusher.Push()
    for{
        metrics_pusher.Push()
        time.Sleep(time.Duration(push_interval) * time.Millisecond)
    }
}

//loop forever, pushing metrics to prometheus defined by interval
// will stop when program stops
func metrics_collect_loop(interval int) {

    for {
        conns, err := netstat.TCP.Connections()
        if err != nil {
            for _, conn := range conns {
                for _, addr := range Monitored_addrs {
                    if conn.RemoteIP.Equal(addr.IP) && conn.RemotePort == addr.Port {
                        TcpSendQueue.WithLabelValues(string(conn.RemoteIP),fmt.Sprint(conn.RemotePort)).Set(float64(conn.TransmitQueue))
                        TcpRecvQueue.WithLabelValues(string(conn.RemoteIP),fmt.Sprint(conn.RemotePort)).Set(float64(conn.ReceiveQueue))
                    }
                }
            }
        }
        
        time.Sleep(time.Duration(interval) * time.Millisecond)

    }
}   

