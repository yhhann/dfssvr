package instrument

import (
	"flag"
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

type Measurements struct {
	Name  string
	Biz   string
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
			Name:      "in_process",
			Help:      "Method in process.",
		},
		[]string{"service"},
	)
	InProcess = make(chan *Measurements, *metricsBufSize)

	// sucLatencyGauge instruments duration of method called successfully.
	sucLatencyGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "dfs2_0",
			Subsystem: "server",
			Name:      "suc_latency_value",
			Help:      "Successful RPC latency gauge in millisecond.",
		},
		[]string{"service"},
	)
	// sucLatency instruments duration distribution of method called successfully.
	sucLatency = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "dfs2_0",
			Subsystem: "server",
			Name:      "suc_latency",
			Help:      "Successful RPC latency in millisecond.",
			Buckets:   prometheus.ExponentialBuckets(0.1, 10, 6),
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

	// timeoutGauge instruments timeout of method.
	timeoutGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "dfs2_0",
			Subsystem: "server",
			Name:      "timeout_value",
			Help:      "timeout gauge in millisecond.",
		},
		[]string{"service"},
	)
	// timeoutHistogram instruments timeout distribution of method.
	timeoutHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "dfs2_0",
			Subsystem: "server",
			Name:      "timeout",
			Help:      "timeout in millisecond.",
			Buckets:   prometheus.ExponentialBuckets(0.1, 10, 6),
		},
		[]string{"service"},
	)
	TimeoutHistogram = make(chan *Measurements, *metricsBufSize)

	// transferRate instruments rate of file transfer.
	transferRate = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "dfs2_0",
			Subsystem: "server",
			Name:      "transfer_rate",
			Help:      "transfer rate in kbit/sec.",
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
		[]string{"service", "biz"},
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

	createdSessionGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "dfs2_0",
			Subsystem: "server",
			Name:      "session_created",
			Help:      "number of created session.",
		},
		[]string{"uri"},
	)
	IncCreated = make(chan *Measurements, *metricsBufSize)
	DecCreated = make(chan *Measurements, *metricsBufSize)

	copiedSessionGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "dfs2_0",
			Subsystem: "server",
			Name:      "session_copied",
			Help:      "number of copied session.",
		},
		[]string{"uri"},
	)
	IncCopied = make(chan *Measurements, *metricsBufSize)
	DecCopied = make(chan *Measurements, *metricsBufSize)

	clonedSessionGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "dfs2_0",
			Subsystem: "server",
			Name:      "session_cloned",
			Help:      "number of cloned session.",
		},
		[]string{"uri"},
	)
	IncCloned = make(chan *Measurements, *metricsBufSize)
	DecCloned = make(chan *Measurements, *metricsBufSize)

	prejudgeExceedCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "dfs2_0",
			Subsystem: "server",
			Name:      "prejudge_exceed_counter",
			Help:      "prejudge exceed counter.",
		},
		[]string{"service"},
	)
	PrejudgeExceed = make(chan *Measurements, *metricsBufSize)

	flagGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "dfs2_0",
			Subsystem: "server",
			Name:      "flag",
			Help:      "flag gauge",
		},
		[]string{"flagkey"},
	)
	FlagGauge = make(chan *Measurements, *metricsBufSize)

	backstoreFileCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "dfs2_0",
			Subsystem: "server",
			Name:      "backstore_file_counter",
			Help:      "Backstore file counter",
		},
		[]string{"service"},
	)
	BackstoreFileCounter = make(chan *Measurements, *metricsBufSize)
)

func init() {
	prometheus.MustRegister(inProcessGauge)
	prometheus.MustRegister(sucLatency)
	prometheus.MustRegister(sucLatencyGauge)
	prometheus.MustRegister(failCounter)
	prometheus.MustRegister(timeoutHistogram)
	prometheus.MustRegister(timeoutGauge)
	prometheus.MustRegister(transferRate)
	prometheus.MustRegister(fileSize)
	prometheus.MustRegister(noDeadlineCounter)
	prometheus.MustRegister(storageStatusGauge)
	prometheus.MustRegister(createdSessionGauge)
	prometheus.MustRegister(copiedSessionGauge)
	prometheus.MustRegister(clonedSessionGauge)
	prometheus.MustRegister(prejudgeExceedCounter)
	prometheus.MustRegister(flagGauge)
	prometheus.MustRegister(backstoreFileCounter)
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
					// in millisecond
					timeoutHistogram.WithLabelValues(m.Name).Observe(m.Value / 1e6)
					timeoutGauge.WithLabelValues(m.Name).Set(m.Value / 1e6)
				case m := <-TransferRate:
					transferRate.WithLabelValues(m.Name).Set(m.Value)
				case m := <-FileSize:
					fileSize.WithLabelValues(m.Name, m.Biz).Observe(m.Value)
				case m := <-SuccessDuration:
					// in millisecond
					sucLatency.WithLabelValues(m.Name).Observe(m.Value / 1e6)
					sucLatencyGauge.WithLabelValues(m.Name).Set(m.Value / 1e6)
				case m := <-StorageStatus:
					storageStatusGauge.WithLabelValues(m.Name).Set(m.Value)
				case m := <-IncCreated:
					createdSessionGauge.WithLabelValues(m.Name).Inc()
				case m := <-DecCreated:
					createdSessionGauge.WithLabelValues(m.Name).Dec()
				case m := <-IncCopied:
					copiedSessionGauge.WithLabelValues(m.Name).Inc()
				case m := <-DecCopied:
					copiedSessionGauge.WithLabelValues(m.Name).Dec()
				case m := <-IncCloned:
					clonedSessionGauge.WithLabelValues(m.Name).Inc()
				case m := <-DecCloned:
					clonedSessionGauge.WithLabelValues(m.Name).Dec()
				case m := <-PrejudgeExceed:
					prejudgeExceedCounter.WithLabelValues(m.Name).Inc()
				case m := <-FlagGauge:
					flagGauge.WithLabelValues(m.Name).Set(m.Value)
				case m := <-BackstoreFileCounter:
					backstoreFileCounter.WithLabelValues(m.Name).Inc()
				}
			}
		}()

		http.Handle(*metricsPath, prometheus.UninstrumentedHandler())
		http.ListenAndServe(*metricsAddr, nil)
	}()
}

func GetTransferRate(method string) (float64, error) {
	sum, err := transferRate.GetMetricWith(prometheus.Labels{"service": method})
	if err != nil {
		return 0, err
	}

	m := &dto.Metric{}
	if err = sum.Write(m); err != nil {
		return 0, err
	}

	return m.GetGauge().GetValue(), nil
}
