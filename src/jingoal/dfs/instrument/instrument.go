package instrument

import (
	"flag"
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
)

type Measurements struct {
	Name  string
	Value float64
}

var (
	metricsAddr    = flag.String("metrics-address", ":8080", "The address to listen on for metrics.")
	metricsPath    = flag.String("metrics-path", "/metrics", "The path of metrics.")
	metricsBufSize = flag.Int("metrics-buf-size", 100, "Size of metrics buffer")
)

var (
	inProcessGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "dfs2_0",
			Subsystem: "server",
			Name:      "in_process_counter",
			Help:      "Method in process.",
		},
		[]string{"service"},
	)
	InProcess = make(chan *Measurements, *metricsBufSize)

	// sucDuration instruments duration of method called successfully.
	sucDuration = prometheus.NewSummaryVec(
		prometheus.SummaryOpts{
			Namespace: "dfs2_0",
			Subsystem: "server",
			Name:      "suc_durations_nanoseconds",
			Help:      "Successful RPC latency distributions.",
		},
		[]string{"service"},
	)
	SuccessDuration = make(chan *Measurements, *metricsBufSize)

	// failCounter instruments number of failed.
	failCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "dfs2_0",
			Subsystem: "server",
			Name:      "fail_counter",
			Help:      "Failed RPC counter.",
		},
		[]string{"service"},
	)
	FailedCounter = make(chan *Measurements, *metricsBufSize)

	// timeoutHistogram instruments timeout of method.
	timeoutHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "dfs2_0",
			Subsystem: "server",
			Name:      "timeout_nanoseconds",
			Help:      "timeout distributions.",
			Buckets:   prometheus.ExponentialBuckets(100000, 10, 6),
		},
		[]string{"service"},
	)
	TimeoutHistogram = make(chan *Measurements, *metricsBufSize)

	// rateHistogram instruments rate of file transfer.
	rateHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "dfs2_0",
			Subsystem: "server",
			Name:      "rate_in_kbit_per_sec",
			Help:      "transfer rate distributions.",
			Buckets:   prometheus.ExponentialBuckets(100*1024, 2, 6),
		},
		[]string{"service"},
	)
	TransferRate = make(chan *Measurements, *metricsBufSize)

	// sizeHistogram instruments size of file.
	sizeHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "dfs2_0",
			Subsystem: "server",
			Name:      "size_in_bytes",
			Help:      "file size distributions.",
			Buckets:   prometheus.ExponentialBuckets(100*1024, 2, 6),
		},
		[]string{"service"},
	)
	FileSize = make(chan *Measurements, *metricsBufSize)

	// noDeadlineCounter instruments number of method which without deadline.
	noDeadlineCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "dfs2_0",
			Subsystem: "server",
			Name:      "no_deadline_counter",
			Help:      "no deadlinecounter.",
		},
		[]string{"service"},
	)
	NoDeadlineCounter = make(chan *Measurements, *metricsBufSize)
)

func init() {
	prometheus.MustRegister(inProcessGauge)
	prometheus.MustRegister(sucDuration)
	prometheus.MustRegister(failCounter)
	prometheus.MustRegister(timeoutHistogram)
	prometheus.MustRegister(rateHistogram)
	prometheus.MustRegister(sizeHistogram)
	prometheus.MustRegister(noDeadlineCounter)
}

func StartMetrics() {
	go func() {
		go func() {
			for {
				select {
				case m := <-InProcess:
					inProcessGauge.WithLabelValues(m.Name).Add(m.Value)
				case m := <-FailedCounter:
					failCounter.WithLabelValues(m.Name).Inc()
				case m := <-NoDeadlineCounter:
					noDeadlineCounter.WithLabelValues(m.Name).Inc()
				case m := <-TimeoutHistogram:
					timeoutHistogram.WithLabelValues(m.Name).Observe(m.Value)
				case m := <-TransferRate:
					rateHistogram.WithLabelValues(m.Name).Observe(m.Value)
				case m := <-FileSize:
					sizeHistogram.WithLabelValues(m.Name).Observe(m.Value)
				case m := <-SuccessDuration:
					sucDuration.WithLabelValues(m.Name).Observe(m.Value)
				}
			}
		}()

		http.Handle(*metricsPath, prometheus.Handler())
		http.ListenAndServe(*metricsAddr, nil)
	}()
}
