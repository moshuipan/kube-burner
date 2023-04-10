package measurements

import (
	"context"
	"math"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/cloud-bulldozer/kube-burner/log"
	"github.com/cloud-bulldozer/kube-burner/pkg/measurements/metrics"
	"github.com/cloud-bulldozer/kube-burner/pkg/measurements/types"
	"github.com/elliotchance/orderedmap/v2"
	probing "github.com/prometheus-community/pro-bing"
	v1 "k8s.io/api/core/v1"
	"k8s.io/client-go/tools/cache"
	kvv1 "kubevirt.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	migrateMeasurement          = "migrateLatencyMeasurement"
	migrateQuantilesMeasurement = "migrateLatencyQuantilesMeasurement"
)

// migrateMetric holds both vmi migrate metrics
type migrateMetric struct {
	// Timestamp filed is very important the the elasticsearch indexing and represents the first creation time that we track (i.e., vm or vmi)
	Timestamp time.Time `json:"timestamp"`

	vmimPending            time.Time
	VMIMPendingLatency     int `json:"vmimPendingLatency"`
	vmimScheduling         time.Time
	VMIMSchedulingLatency  int `json:"vmimSchedulingLatency"`
	vmimScheduled          time.Time
	VMIMScheduledLatency   int `json:"vmimScheduledLatency"`
	vmimPreparing          time.Time
	VMIMPreparingLatency   int `json:"vmimPreparingLatency"`
	vmimTargetReady        time.Time
	VMIMTargetReadyLatency int `json:"vmimTargetReadyLatency"`
	vmimRunning            time.Time
	VMIMRunningLatency     int `json:"vmimRunningLatency"`
	vmimSucceeded          time.Time
	VMIMSucceededLatency   int `json:"vmimSucceededLatency"`
	firstLoss              time.Time
	lastLoss               time.Time
	LossDuration           int `json:"lossDuration"`

	MetricName string `json:"metricName"`
	JobName    string `json:"jobName"`
	UUID       string `json:"uuid"`
	Namespace  string `json:"namespace"`
	NodeName   string `json:"nodeName"`
	Name       string `json:"name"`
}

type migrateLatency struct {
	config           types.Measurement
	migrateWatcher   *metrics.Watcher
	metrics          map[string]*migrateMetric
	latencyQuantiles []interface{}
	normLatencies    []interface{}
	mu               sync.Mutex
	cli              client.Client
	wg               sync.WaitGroup
}

func init() {
	measurementMap["migrateLatency"] = &migrateLatency{}
}

func (p *migrateLatency) startPing(vmID string, vmim *kvv1.VirtualMachineInstanceMigration) {
	defer p.wg.Done()
	vmi := &kvv1.VirtualMachineInstance{}
	err := p.cli.Get(context.Background(), client.ObjectKey{Name: vmim.Spec.VMIName, Namespace: vmim.Namespace}, vmi)
	if err != nil {
		log.Errorf("failed to start ping measure,vmi:%s,err:%v\n", vmim.Spec.VMIName, err)
		return
	}
	if len(vmi.Status.Interfaces) <= 0 {
		log.Errorf("failed to start ping measure,vmi:%s,err:can't get vmi ip\n", vmim.Spec.VMIName)
		return
	}
	ip := vmi.Status.Interfaces[0].IP
	pinger, err := probing.NewPinger(ip)
	if err != nil {
		log.Errorf("failed to start pinger,vmi:%s,err:can't get vmi ip\n", vmim.Spec.VMIName)
		return
	}
	pinger.Interval = time.Millisecond * 10
	pinger.SetPrivileged(true)
	type tmp struct {
		seq int
		t   time.Time
	}
	l := sync.Mutex{}
	m := orderedmap.NewOrderedMap[int, *tmp]()

	timeout := time.After(time.Minute * 10)
	go func() {
		tick := time.NewTicker(time.Second * 3)
		for range tick.C {
			l.Lock()
			f := m.Front()
			if f == nil {
				l.Unlock()
				continue
			}
			now := time.Now()
			if now.Sub(f.Value.t) > time.Second*2 {
				last := m.Back()
				if now.Sub(last.Value.t) > time.Second*2 {
					pinger.Stop()
					l.Unlock()
					return
				}
			}
			l.Unlock()
			select {
			case <-timeout:
				log.Infof("net downtime <10ms,vmi:%s\n", vmi.Name)
				return
			default:
			}
		}
	}()
	pinger.OnSend = func(packet *probing.Packet) {
		l.Lock()
		m.Set(packet.Seq, &tmp{seq: packet.Seq, t: time.Now()})
		l.Unlock()
	}
	pinger.OnRecv = func(pkt *probing.Packet) {
		l.Lock()
		m.Delete(pkt.Seq)
		l.Unlock()
	}
	log.Printf("PING %s (%s)\n", pinger.Addr(), pinger.IPAddr())
	err = pinger.Run()
	if err != nil {
		log.Errorf("Failed to ping target host:%v,vmi:%s\n", err, vmi.Name)
	}
	firstLoss := m.Front().Value
	lastLoss := m.Back().Value
	if firstLoss == nil {
		now := &tmp{
			seq: 0,
			t:   time.Now(),
		}
		firstLoss, lastLoss = now, now
	}

	p.mu.Lock()
	if vmiM, exists := p.metrics[vmID]; exists {
		vmiM.firstLoss, vmiM.lastLoss = firstLoss.t, lastLoss.t
	}
	p.mu.Unlock()
}

func (p *migrateLatency) handleCreateVMIM(obj interface{}) {
	vmim := obj.(*kvv1.VirtualMachineInstanceMigration)
	vmID := vmim.Labels["kubevirt-vm"]
	p.mu.Lock()
	if _, exists := p.metrics[vmID]; !exists {
		if strings.Contains(vmim.Namespace, factory.jobConfig.Namespace) {
			p.wg.Add(1)
			go p.startPing(vmID, vmim)
			p.metrics[vmID] = &migrateMetric{
				Timestamp:  time.Now().UTC(),
				Namespace:  vmim.Namespace,
				Name:       vmim.Name,
				MetricName: migrateMeasurement,
				UUID:       factory.uuid,
				JobName:    factory.jobConfig.Name,
			}
		}
	}
	p.mu.Unlock()
}

func (p *migrateLatency) handleUpdateVMIM(obj interface{}) {
	var vmID string
	vmim := obj.(*kvv1.VirtualMachineInstanceMigration)
	// in case the parent is a VM object
	if id, exists := vmim.Labels["kubevirt-vm"]; exists {
		vmID = id
	}
	// in case the parent is a VMI object
	if vmID == "" {
		vmID = string(vmim.UID)
	}
	p.mu.Lock()
	if vmiM, exists := p.metrics[vmID]; exists && vmiM.vmimSucceeded.IsZero() {
		switch vmim.Status.Phase {
		case kvv1.MigrationPending:
			if vmiM.vmimPending.IsZero() {
				vmiM.vmimPending = time.Now().UTC()
			}
		case kvv1.MigrationScheduling:
			if vmiM.vmimScheduling.IsZero() {
				vmiM.vmimScheduling = time.Now().UTC()
			}
		case kvv1.MigrationScheduled:
			if vmiM.vmimScheduled.IsZero() {
				vmiM.vmimScheduled = time.Now().UTC()
			}
		case kvv1.MigrationPreparingTarget:
			if vmiM.vmimPreparing.IsZero() {
				vmiM.vmimPreparing = time.Now().UTC()
			}
		case kvv1.MigrationTargetReady:
			if vmiM.vmimTargetReady.IsZero() {
				vmiM.vmimTargetReady = time.Now().UTC()
			}
		case kvv1.MigrationRunning:
			if vmiM.vmimRunning.IsZero() {
				vmiM.vmimRunning = time.Now().UTC()
			}
		case kvv1.MigrationSucceeded:
			if vmiM.vmimSucceeded.IsZero() {
				vmiM.vmimSucceeded = time.Now().UTC()
			}
		}
	}
	p.mu.Unlock()
}

func (p *migrateLatency) setConfig(cfg types.Measurement) error {
	p.config = cfg
	return nil
}

// Start starts migrateLatency measurement
func (p *migrateLatency) start(measurementWg *sync.WaitGroup) {
	defer measurementWg.Done()
	p.metrics = make(map[string]*migrateMetric)

	log.Infof("Creating VMIM latency watcher for %s", factory.jobConfig.Name)
	restClient, cli := newRESTClientWithRegisteredKubevirtResource()
	p.cli = cli
	p.migrateWatcher = metrics.NewWatcher(
		restClient,
		"migrateWatcher",
		"virtualmachineinstancemigrations",
		v1.NamespaceAll,
	)
	p.migrateWatcher.Informer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: p.handleCreateVMIM,
		UpdateFunc: func(oldObj, newObj interface{}) {
			p.handleUpdateVMIM(newObj)
		},
	})
	if err := p.migrateWatcher.StartAndCacheSync(); err != nil {
		log.Errorf("VMIM Latency measurement error: %s", err)
	}
}

// Stop stops vmiLatency measurement
func (p *migrateLatency) stop() (int, error) {
	//wait all ping stop
	p.wg.Wait()

	p.migrateWatcher.StopWatcher()
	p.normalizeMetrics()
	p.calcQuantiles()
	rc := metrics.CheckThreshold(p.config.LatencyThresholds, p.latencyQuantiles)
	if kubeburnerCfg.WriteToFile {
		if err := metrics.WriteToFile(p.normLatencies, p.latencyQuantiles, "migrateLatency", factory.jobConfig.Name, kubeburnerCfg.MetricsDirectory); err != nil {
			log.Errorf("Error writing measurement migrateLatency: %s", err)
		}
	}
	if kubeburnerCfg.IndexerConfig.Enabled {
		p.index()
	}
	// Reset latency slices, required in multi-job benchmarks
	p.latencyQuantiles = nil
	p.normLatencies = nil
	return rc, nil
}

// index sends metrics to the configured indexer
func (p *migrateLatency) index() {
	(*factory.indexer).Index(p.config.ESIndex, p.normLatencies)
	(*factory.indexer).Index(p.config.ESIndex, p.latencyQuantiles)
}

func (p *migrateLatency) normalizeMetrics() {
	for _, m := range p.metrics {
		// If a does not reach the Ready state (this timestamp wasn't set), we skip that vmi
		if m.vmimSucceeded.IsZero() {
			continue
		}
		m.VMIMPendingLatency = int(m.vmimPending.Sub(m.Timestamp).Milliseconds())
		m.VMIMSchedulingLatency = int(m.vmimScheduling.Sub(m.Timestamp).Milliseconds())
		m.VMIMScheduledLatency = int(m.vmimScheduled.Sub(m.Timestamp).Milliseconds())
		m.VMIMPreparingLatency = int(m.vmimPreparing.Sub(m.Timestamp).Milliseconds())
		m.VMIMTargetReadyLatency = int(m.vmimTargetReady.Sub(m.Timestamp).Milliseconds())
		m.VMIMRunningLatency = int(m.vmimRunning.Sub(m.Timestamp).Milliseconds())
		m.VMIMSucceededLatency = int(m.vmimSucceeded.Sub(m.Timestamp).Milliseconds())
		m.LossDuration = int(m.lastLoss.Sub(m.firstLoss).Milliseconds())

		p.normLatencies = append(p.normLatencies, m)
	}
}

func (p *migrateLatency) calcQuantiles() {
	quantiles := []float64{0.5, 0.95, 0.99}
	quantileMap := map[string][]int{}
	for _, normLatency := range p.normLatencies {
		quantileMap["VMIM"+string(kvv1.MigrationPending)] = append(quantileMap["VMIM"+string(kvv1.MigrationPending)], normLatency.(*migrateMetric).VMIMPendingLatency)
		quantileMap["VMIM"+string(kvv1.MigrationScheduling)] = append(quantileMap["VMIM"+string(kvv1.MigrationScheduling)], normLatency.(*migrateMetric).VMIMSchedulingLatency)
		quantileMap["VMIM"+string(kvv1.MigrationScheduled)] = append(quantileMap["VMIM"+string(kvv1.MigrationScheduled)], normLatency.(*migrateMetric).VMIMScheduledLatency)
		quantileMap["VMIM"+string(kvv1.MigrationPreparingTarget)] = append(quantileMap["VMIM"+string(kvv1.MigrationPreparingTarget)], normLatency.(*migrateMetric).VMIMPreparingLatency)
		quantileMap["VMIM"+string(kvv1.MigrationTargetReady)] = append(quantileMap["VMIM"+string(kvv1.MigrationTargetReady)], normLatency.(*migrateMetric).VMIMTargetReadyLatency)
		quantileMap["VMIM"+string(kvv1.MigrationRunning)] = append(quantileMap["VMIM"+string(kvv1.MigrationRunning)], normLatency.(*migrateMetric).VMIMRunningLatency)
		quantileMap["VMIM"+string(kvv1.MigrationSucceeded)] = append(quantileMap["VMIM"+string(kvv1.MigrationSucceeded)], normLatency.(*migrateMetric).VMIMSucceededLatency)
		quantileMap["VMIMLossDuration"] = append(quantileMap["VMIMLossDuration"], normLatency.(*migrateMetric).LossDuration)
	}
	for quantileName, v := range quantileMap {
		quantile := metrics.LatencyQuantiles{
			QuantileName: quantileName,
			UUID:         factory.uuid,
			Timestamp:    time.Now().UTC(),
			JobName:      factory.jobConfig.Name,
			MetricName:   migrateQuantilesMeasurement,
		}
		sort.Ints(v)
		length := len(v)
		if length > 1 {
			for _, q := range quantiles {
				qValue := v[int(math.Ceil(float64(length)*q))-1]
				quantile.SetQuantile(q, qValue)
			}
			quantile.Max = v[length-1]
		}
		sum := 0
		for _, n := range v {
			sum += n
		}
		quantile.Avg = int(math.Round(float64(sum) / float64(length)))
		p.latencyQuantiles = append(p.latencyQuantiles, quantile)
	}
}
