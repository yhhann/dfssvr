package instrument

import (
	"flag"
	"fmt"
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

type Measurements struct {
	Name  string
	Value float64
}

var (
	metricsAddr    = flag.String("metrics-address", ":8080", "The address to listen on for metrics.")
	metricsPath    = flag.String("metrics-path", "/dfs-metrics", "The path of metrics.")
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

	// transferRate instruments rate of file transfer.
	transferRate = prometheus.NewSummaryVec(
		prometheus.SummaryOpts{
			Namespace: "dfs2_0",
			Subsystem: "server",
			Name:      "rate_in_kbit_per_sec",
			Help:      "transfer rate distributions.",
		},
		[]string{"service"},
	)
	TransferRate = make(chan *Measurements, *metricsBufSize)

	// fileSize instruments size of file.
	fileSize = prometheus.NewHistogramVec(
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
			Help:      "no deadline counter.",
		},
		[]string{"service"},
	)
	NoDeadlineCounter = make(chan *Measurements, *metricsBufSize)

	// storageStatusGauge instruments status of storage server.
	storageStatusGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "dfs2_0",
			Subsystem: "server",
			Name:      "storage_status",
			Help:      "Storage status.",
		},
		[]string{"service"},
	)
	StorageStatus = make(chan *Measurements, *metricsBufSize)
)

func init() {
	prometheus.MustRegister(inProcessGauge)
	prometheus.MustRegister(sucDuration)
	prometheus.MustRegister(failCounter)
	prometheus.MustRegister(timeoutHistogram)
	prometheus.MustRegister(transferRate)
	prometheus.MustRegister(fileSize)
	prometheus.MustRegister(noDeadlineCounter)
	prometheus.MustRegister(storageStatusGauge)
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
					transferRate.WithLabelValues(m.Name).Observe(m.Value)
				case m := <-FileSize:
					fileSize.WithLabelValues(m.Name).Observe(m.Value)
				case m := <-SuccessDuration:
					sucDuration.WithLabelValues(m.Name).Observe(m.Value)
				case m := <-StorageStatus:
					storageStatusGauge.WithLabelValues(m.Name).Set(m.Value)
				}
			}
		}()

		http.Handle(*metricsPath, prometheus.UninstrumentedHandler())
		http.ListenAndServe(*metricsAddr, nil)
	}()
}

func GetTransferRateQuantile(method string, quantile float64) (float64, error) {
	sum, err := transferRate.GetMetricWith(prometheus.Labels{"service": method})
	if err != nil {
		return 0, err
	}

	m := &dto.Metric{}
	if err = sum.Write(m); err != nil {
		return 0, err
	}

	qs := m.GetSummary().GetQuantile()
	for _, q := range qs {
		if q.GetQuantile() == quantile {
			return q.GetValue(), nil
		}
	}

	return 0, fmt.Errorf("Not Found")
}
