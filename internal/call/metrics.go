package call

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Direct-media setup and cleanup metrics. Packet and loss counters for direct
// calls come from the LiveKit server, which terminates the media.
var (
	directSetups = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "whatsapp_direct_setup_total",
		Help: "Direct media call setups by result (connected or a failure reason).",
	}, []string{"result"})
	directAgentWait = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "whatsapp_direct_agent_ready_seconds",
		Help:    "Time from accept until the agent joined (and published its audio when the connector waits for it).",
		Buckets: []float64{0.25, 0.5, 1, 2, 4, 8, 16},
	})
	directWHIP = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "whatsapp_direct_whip_seconds",
		Help:    "Time the LiveKit WHIP request took to return the SDP answer.",
		Buckets: []float64{0.25, 0.5, 1, 2, 3, 5, 8, 12},
	})
	directAnswer = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "whatsapp_direct_answer_seconds",
		Help:    "Time from accept until Meta had the SDP answer (pre_accept sent).",
		Buckets: []float64{0.5, 1, 2, 3, 5, 8, 12, 20},
	})
	directCleanups = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "whatsapp_direct_cleanup_total",
		Help: "Direct media session cleanups by result.",
	}, []string{"result"})
)
