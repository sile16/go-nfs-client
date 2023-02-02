package metrics

import (
	"bytes"
	"fmt"
	"net"
	"os/exec"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/bastjan/netstat"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/push"
)

var (
    MC = map[string]prometheus.Counter{
        "RpcRequestsCounter": prometheus.NewCounter(prometheus.CounterOpts{
            Name: "rpc_requests_total",
            Help: "The total number of rpc requests",
        }),
        "RpcWriteRequestsCounter": prometheus.NewCounter(prometheus.CounterOpts{
            Name: "rpc_write_requests_total",
            Help: "The total number of write rpc requests",
        }),
        "RpcReadRequestsCounter": prometheus.NewCounter(prometheus.CounterOpts{
            Name: "rpc_read_requests_total",
            Help: "The total number of read rpc requests",
        }),
        "RpcBytesReadCounter": prometheus.NewCounter(prometheus.CounterOpts{
            Name: "rpc_bytes_read_total",
            Help: "The total number of bytes read",
        }),
        "RpcBytesWrittenCounter": prometheus.NewCounter(prometheus.CounterOpts{
            Name: "rpc_bytes_written_total",
            Help: "The total number of bytes written",
        }),
    }

    MG = map[string]prometheus.Gauge{
        "RpcOutstandingRequests": prometheus.NewGauge(prometheus.GaugeOpts{
            Name: "rpc_outstanding_requests",
            Help: "The total number of outstanding rpc requests",
        }),
    }

    MGV = map[string]*prometheus.GaugeVec{
        "TcpSendQueue": prometheus.NewGaugeVec(prometheus.GaugeOpts{
            Name: "tcp_send_queue",
            Help: "The total number of outstanding rpc requests",
        }, []string{"remote_addr", "remote_port"}),

        "TcpRecvQueue": prometheus.NewGaugeVec(prometheus.GaugeOpts{
            Name: "tcp_recv_queue",
            Help: "The total number of outstanding rpc requests",
        }, []string{"remote_addr", "remote_port"}),
    }

    MH = map[string]prometheus.Histogram{
        "RpcRequestLatencyuS":  prometheus.NewHistogram(prometheus.HistogramOpts{
            Name:    "rpc_latency_us",
            Help:    "Histogram of rpc latency in us", // Sorry, we can't measure how badly it smells.
            //Buckets: prometheus.LinearBuckets(500, 5, 5),  // 5 buckets, each 5 centigrade wide.
            Buckets: prometheus.ExponentialBuckets(500, 2, 6),  // 10 buckets, each 2x wider.
        }),
        "RpcIOSizeRequest":  prometheus.NewHistogram(prometheus.HistogramOpts{
            Name:    "rpc_request_io_size",
            Help:    "Histogram of rpc request size", // Sorry, we can't measure how badly it smells.
            //Buckets: prometheus.LinearBuckets(500, 5, 5),  // 5 buckets, each 5 centigrade wide.
            Buckets: []float64{8*1024, 64*1024, 128*1024, 256*1024, 512*1024, 1024*1024}, 
        }),
        "RpcIOSizeReceive":  prometheus.NewHistogram(prometheus.HistogramOpts{
            Name:    "rpc_receive_io_size",
            Help:    "Histogram of rpc receive size", // Sorry, we can't measure how badly it smells.
            //Buckets: prometheus.LinearBuckets(500, 5, 5),  // 5 buckets, each 5 centigrade wide.
            Buckets: []float64{8*1024, 64*1024, 128*1024, 256*1024, 512*1024, 1024*1024}, 
        }),

    }

	metrics_pusher *push.Pusher

    //a thread safe list []*net.TCPaddr] to be monitored
    Monitored_addrs = make([]*net.TCPAddr ,0)
)

func Metrics_init(job_name string, push_interval int) {

    metrics_pusher = push.New("http://127.0.0.1:9091", job_name)
    
    for _ , m := range MC {
        prometheus.MustRegister(m)
        metrics_pusher.Collector(m)
    }

    for _ , m := range MG {
        prometheus.MustRegister(m)
        metrics_pusher.Collector(m)
    }

    for _ , m := range MGV {
        prometheus.MustRegister(m)
        metrics_pusher.Collector(m)
    }

    for _ , m := range MH {
        prometheus.MustRegister(m)
        metrics_pusher.Collector(m)
    }

    MG["RpcOutstandingRequests"].Set(0)
    
    // Register the default golang metrics with the Prometheus registry.

    systems_collectors := []prometheus.Collector{
        collectors.NewGoCollector(),
        collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
    }
    
    for _, c := range systems_collectors {
        //prometheus.MustRegister(c)
        metrics_pusher.Collector(c)
    }

    go metrics_collect_loop(push_interval)
    go Metrics_push(push_interval)
}

func Metrics_push(push_interval int) {
    // one last push to capture ending metrics.
    defer zero_guages()
    defer metrics_pusher.Push()

    for{
        // Gather the registered metrics.
        prometheus.DefaultGatherer.Gather()
        metrics_pusher.Push()
        time.Sleep(time.Duration(push_interval) * time.Millisecond)
    }
}

//loop forever, pushing metrics to prometheus defined by interval
// will stop when program stops
func metrics_collect_loop(interval int) {

    for {
        var conns []*netstat.Connection
        var err error


        if len(Monitored_addrs) > 1 {
            if runtime.GOOS == "darwin" {
                conns, err = get_mac_connection_info()
            } else {
                conns, err = netstat.TCP.Connections()
            }

            if err == nil {
                for _, conn := range conns {
                    for _, addr := range Monitored_addrs {
                        if conn.RemoteIP.Equal(addr.IP) && conn.RemotePort == addr.Port {
                            remote_ip := conn.RemoteIP.String()
                            remote_port := fmt.Sprint(conn.RemotePort)
                            MGV["TcpSendQueue"].WithLabelValues(remote_ip, remote_port).Set(float64(conn.TransmitQueue))
                            MGV["TcpRecvQueue"].WithLabelValues(remote_ip, remote_port).Set(float64(conn.ReceiveQueue))
                        }
                    }
                }
            }
        }
        
        time.Sleep(time.Duration(interval) * time.Millisecond)
    }
} 

func zero_guages() {
    for _, addr := range Monitored_addrs {
        remote_ip := addr.IP.String()
        remote_port := fmt.Sprint(addr.Port)
        MGV["TcpSendQueue"].WithLabelValues(remote_ip, remote_port).Set(float64(0))
        MGV["TcpRecvQueue"].WithLabelValues(remote_ip, remote_port).Set(float64(0))
        
    }

    MG["RpcOutstandingRequests"].Set(0)
}

// get macosx tcp connection info
func get_mac_connection_info() ([]*netstat.Connection, error) {
    //netstat -an -p tcp | grep

    cmd := exec.Command("netstat", "-anp", "tcp", "-f", "inet")
	var out bytes.Buffer
	cmd.Stdout = &out

	err := cmd.Run()
	if err != nil {
		return nil, err
	}

    conns := make([]*netstat.Connection, 0)

	// Parse the output
	lines := strings.Split(out.String(), "\n")
	for _, line := range lines[2:] {
		fields := strings.Fields(line)
		if len(fields) < 5 {
			continue
		}

		// Convert the fields to the specified types
        recvQ, _ := strconv.Atoi(fields[1])
		sendQ, _ := strconv.Atoi(fields[2])
		localAddr, err := net.ResolveTCPAddr("tcp", replaceFirstReverse(fields[3], '.', ':'))
        if err != nil {
            continue
        }
		foreignAddr,err := net.ResolveTCPAddr("tcp",replaceFirstReverse(fields[4], '.', ':'))
        if err != nil {
            continue
        }
		//state := fields[5]

        conns = append(conns, &netstat.Connection{
            Protocol:  &netstat.Protocol{Name: "tcp"},
            ReceiveQueue: uint64(recvQ),
            TransmitQueue: uint64(sendQ),
            IP: localAddr.IP,
            Port: localAddr.Port,
            RemoteIP: foreignAddr.IP,
            RemotePort: foreignAddr.Port,
        })
	}

    return conns, nil
}

func replaceFirstReverse(s string, from rune, to rune) string {
    runes := []rune(s)
    for i := len(runes) - 1; i >= 0; i-- {
        if runes[i] == from {
            runes[i] = to
            break
        }
    }
    return string(runes)
}

// netstat -sp tcp -f inet
// get tcp retra






