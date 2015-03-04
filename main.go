// Copyright 2013 The Prometheus Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"flag"
	_ "net/http/pprof" // Comment this line to disable pprof endpoint.
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/golang/glog"

	clientmodel "github.com/prometheus/client_golang/model"
	registry "github.com/prometheus/client_golang/prometheus"

	"github.com/prometheus/prometheus/config"
	"github.com/prometheus/prometheus/notification"
	"github.com/prometheus/prometheus/retrieval"
	"github.com/prometheus/prometheus/rules/manager"
	"github.com/prometheus/prometheus/storage/local"
	"github.com/prometheus/prometheus/storage/remote"
	"github.com/prometheus/prometheus/storage/remote/opentsdb"
	"github.com/prometheus/prometheus/web"
	"github.com/prometheus/prometheus/web/api"
)

const deletionBatchSize = 100

// Commandline flags.
var (
	configFile = flag.String("config.file", "prometheus.conf", "Prometheus configuration file name.")

	alertmanagerURL           = flag.String("alertmanager.url", "", "The URL of the alert manager to send notifications to.")
	notificationQueueCapacity = flag.Int("alertmanager.notification-queue-capacity", 100, "The capacity of the queue for pending alert manager notifications.")

	persistenceStoragePath = flag.String("storage.local.path", "/tmp/metrics", "Base path for metrics storage.")

	remoteTSDBUrl     = flag.String("storage.remote.url", "", "The URL of the OpenTSDB instance to send samples to.")
	remoteTSDBTimeout = flag.Duration("storage.remote.timeout", 30*time.Second, "The timeout to use when sending samples to OpenTSDB.")

	samplesQueueCapacity = flag.Int("storage.incoming-samples-queue-capacity", 64*1024, "The capacity of the queue of samples to be stored. Note that each slot in the queue takes a whole slice of samples whose size depends on details of the scrape process.")

	chunkType       = flag.Int("storage.local.chunk-type", 1, "Which chunk encoding version to use. Currently supported is 0 and 1.")
	numMemoryChunks = flag.Int("storage.local.memory-chunks", 1024*1024, "How many chunks to keep in memory. While the size of a chunk is 1kiB, the total memory usage will be significantly higher than this value * 1kiB. Furthermore, for various reasons, more chunks might have to be kept in memory temporarily.")

	persistenceRetentionPeriod = flag.Duration("storage.local.retention", 15*24*time.Hour, "How long to retain samples in the local storage.")
	persistenceQueueCapacity   = flag.Int("storage.local.persistence-queue-capacity", 32*1024, "How many chunks can be waiting for being persisted before sample ingestion will stop.")

	checkpointInterval         = flag.Duration("storage.local.checkpoint-interval", 5*time.Minute, "The period at which the in-memory index of time series is checkpointed.")
	checkpointDirtySeriesLimit = flag.Int("storage.local.checkpoint-dirty-series-limit", 5000, "If approx. that many time series are in a state that would require a recovery operation after a crash, a checkpoint is triggered, even if the checkpoint interval hasn't passed yet. A recovery operation requires a disk seek. The default limit intends to keep the recovery time below 1min even on spinning disks. With SSD, recovery is much faster, so you might want to increase this value in that case to avoid overly frequent checkpoints.")

	storageDirty = flag.Bool("storage.local.dirty", false, "If set, the local storage layer will perform crash recovery even if the last shutdown appears to be clean.")

	printVersion = flag.Bool("version", false, "Print version information.")
)

// Instrumentation.
var (
	samplesQueueCapDesc = registry.NewDesc(
		"prometheus_samples_queue_capacity",
		"Capacity of the queue for unwritten samples.",
		nil, nil,
	)
	samplesQueueLenDesc = registry.NewDesc(
		"prometheus_samples_queue_length",
		"Current number of items in the queue for unwritten samples. Each item comprises all samples exposed by one target as one metric family (i.e. metrics of the same name).",
		nil, nil,
	)
)

type prometheus struct {
	unwrittenSamples chan clientmodel.Samples

	ruleManager         manager.RuleManager
	targetManager       retrieval.TargetManager
	notificationHandler *notification.NotificationHandler
	storage             local.Storage
	remoteTSDBQueue     *remote.TSDBQueueManager

	webService *web.WebService

	closeOnce sync.Once
}

// NewPrometheus creates a new prometheus object based on flag values.
// Call Serve() to start serving and Close() for clean shutdown.
func NewPrometheus() *prometheus {
	conf, err := config.LoadFromFile(*configFile)
	if err != nil {
		glog.Fatalf("Error loading configuration from %s: %v", *configFile, err)
	}

	unwrittenSamples := make(chan clientmodel.Samples, *samplesQueueCapacity)

	ingester := &retrieval.MergeLabelsIngester{
		Labels:          conf.GlobalLabels(),
		CollisionPrefix: clientmodel.ExporterLabelPrefix,
		Ingester:        retrieval.ChannelIngester(unwrittenSamples),
	}
	targetManager := retrieval.NewTargetManager(ingester)
	targetManager.AddTargetsFromConfig(conf)

	notificationHandler := notification.NewNotificationHandler(*alertmanagerURL, *notificationQueueCapacity)

	o := &local.MemorySeriesStorageOptions{
		MemoryChunks:               *numMemoryChunks,
		PersistenceStoragePath:     *persistenceStoragePath,
		PersistenceRetentionPeriod: *persistenceRetentionPeriod,
		PersistenceQueueCapacity:   *persistenceQueueCapacity,
		CheckpointInterval:         *checkpointInterval,
		CheckpointDirtySeriesLimit: *checkpointDirtySeriesLimit,
		ChunkType:                  byte(*chunkType),
		Dirty:                      *storageDirty,
	}
	memStorage, err := local.NewMemorySeriesStorage(o)
	if err != nil {
		glog.Fatal("Error opening memory series storage: ", err)
	}

	ruleManager := manager.NewRuleManager(&manager.RuleManagerOptions{
		Results:             unwrittenSamples,
		NotificationHandler: notificationHandler,
		EvaluationInterval:  conf.EvaluationInterval(),
		Storage:             memStorage,
		PrometheusURL:       web.MustBuildServerURL(),
	})
	if err := ruleManager.AddRulesFromConfig(conf); err != nil {
		glog.Fatal("Error loading rule files: ", err)
	}

	var remoteTSDBQueue *remote.TSDBQueueManager
	if *remoteTSDBUrl == "" {
		glog.Warningf("No TSDB URL provided; not sending any samples to long-term storage")
	} else {
		openTSDB := opentsdb.NewClient(*remoteTSDBUrl, *remoteTSDBTimeout)
		remoteTSDBQueue = remote.NewTSDBQueueManager(openTSDB, 512)
	}

	flags := map[string]string{}
	flag.VisitAll(func(f *flag.Flag) {
		flags[f.Name] = f.Value.String()
	})
	prometheusStatus := &web.PrometheusStatusHandler{
		BuildInfo:   BuildInfo,
		Config:      conf.String(),
		RuleManager: ruleManager,
		TargetPools: targetManager.Pools(),
		Flags:       flags,
		Birth:       time.Now(),
	}

	alertsHandler := &web.AlertsHandler{
		RuleManager: ruleManager,
	}

	consolesHandler := &web.ConsolesHandler{
		Storage: memStorage,
	}

	metricsService := &api.MetricsService{
		Config:        &conf,
		TargetManager: targetManager,
		Storage:       memStorage,
	}

	webService := &web.WebService{
		StatusHandler:   prometheusStatus,
		MetricsHandler:  metricsService,
		ConsolesHandler: consolesHandler,
		AlertsHandler:   alertsHandler,
	}

	p := &prometheus{
		unwrittenSamples: unwrittenSamples,

		ruleManager:         ruleManager,
		targetManager:       targetManager,
		notificationHandler: notificationHandler,
		storage:             memStorage,
		remoteTSDBQueue:     remoteTSDBQueue,

		webService: webService,
	}
	webService.QuitDelegate = p.Close
	return p
}

// Serve starts the Prometheus server. It returns after the server has been shut
// down. The method installs an interrupt handler, allowing to trigger a
// shutdown by sending SIGTERM to the process.
func (p *prometheus) Serve() {
	if p.remoteTSDBQueue != nil {
		go p.remoteTSDBQueue.Run()
	}
	go p.ruleManager.Run()
	go p.notificationHandler.Run()
	go p.interruptHandler()

	p.storage.Start()

	go func() {
		err := p.webService.ServeForever()
		if err != nil {
			glog.Fatal(err)
		}
	}()

	for samples := range p.unwrittenSamples {
		p.storage.AppendSamples(samples)
		if p.remoteTSDBQueue != nil {
			p.remoteTSDBQueue.Queue(samples)
		}
	}

	// The following shut-down operations have to happen after
	// unwrittenSamples is drained. So do not move them into close().
	if err := p.storage.Stop(); err != nil {
		glog.Error("Error stopping local storage: ", err)
	}

	if p.remoteTSDBQueue != nil {
		p.remoteTSDBQueue.Stop()
	}

	p.notificationHandler.Stop()
	glog.Info("See you next time!")
}

// Close cleanly shuts down the Prometheus server.
func (p *prometheus) Close() {
	p.closeOnce.Do(p.close)
}

func (p *prometheus) interruptHandler() {
	notifier := make(chan os.Signal)
	signal.Notify(notifier, os.Interrupt, syscall.SIGTERM)
	<-notifier

	glog.Warning("Received SIGTERM, exiting gracefully...")
	p.Close()
}

func (p *prometheus) close() {
	glog.Info("Shutdown has been requested; subsytems are closing:")
	p.targetManager.Stop()
	p.ruleManager.Stop()

	close(p.unwrittenSamples)
	// Note: Before closing the remaining subsystems (storage, ...), we have
	// to wait until p.unwrittenSamples is actually drained. Therefore,
	// remaining shut-downs happen in Serve().
}

// Describe implements registry.Collector.
func (p *prometheus) Describe(ch chan<- *registry.Desc) {
	ch <- samplesQueueCapDesc
	ch <- samplesQueueLenDesc
	p.notificationHandler.Describe(ch)
	p.storage.Describe(ch)
	if p.remoteTSDBQueue != nil {
		p.remoteTSDBQueue.Describe(ch)
	}
}

// Collect implements registry.Collector.
func (p *prometheus) Collect(ch chan<- registry.Metric) {
	ch <- registry.MustNewConstMetric(
		samplesQueueCapDesc,
		registry.GaugeValue,
		float64(cap(p.unwrittenSamples)),
	)
	ch <- registry.MustNewConstMetric(
		samplesQueueLenDesc,
		registry.GaugeValue,
		float64(len(p.unwrittenSamples)),
	)
	p.notificationHandler.Collect(ch)
	p.storage.Collect(ch)
	if p.remoteTSDBQueue != nil {
		p.remoteTSDBQueue.Collect(ch)
	}
}

func main() {
	flag.Parse()
	versionInfoTmpl.Execute(os.Stdout, BuildInfo)

	if *printVersion {
		os.Exit(0)
	}

	p := NewPrometheus()
	registry.MustRegister(p)
	p.Serve()
}
