package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"math/rand"
	"net/url"
	"os"
	"strings"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

const randomAlphabet = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

type config struct {
	Phase             string
	BrokerURL         string
	TopicCritical     string
	TopicBest         string
	Columns           int
	Cases             int
	StrLen            int
	QoS               int
	PhaseADurationSec int
	PhaseARateCrit    int
	PhaseARateBest    int
	PhaseBDurationSec int
	PhaseBRateCrit    int
	PhaseBRateBest    int
	ClientPrefix      string
}

type phaseSettings struct {
	Name        string
	DurationSec int
	RateCrit    int
	RateBest    int
}

type publisherResult struct {
	Label    string
	Topic    string
	Rate     int
	Duration time.Duration
	Sent     int64
	Err      error
}

func main() {
	cfg, err := parseFlags()
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(2)
	}

	if err := validateConfig(cfg); err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(2)
	}

	payloads, err := generatePayloadCases(cfg.Columns, cfg.Cases, cfg.StrLen)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(2)
	}
	phases := buildPhases(cfg)
	for _, phase := range phases {
		if err := runPhase(cfg, phase, payloads); err != nil {
			fmt.Fprintf(os.Stderr, "error: %v\n", err)
			os.Exit(2)
		}
	}
	fmt.Printf("[workload] done phase=%s\n", cfg.Phase)
}

func parseFlags() (*config, error) {
	cfg := &config{}

	flag.StringVar(&cfg.Phase, "phase", "both", "a|b|both")
	flag.StringVar(&cfg.BrokerURL, "broker-url", "tcp://127.0.0.1:1883", "MQTT broker URL")
	flag.StringVar(&cfg.TopicCritical, "topic-critical", "/perf/pr/critical", "critical topic")
	flag.StringVar(&cfg.TopicBest, "topic-best", "/perf/pr/best", "best-effort topic")
	flag.IntVar(&cfg.Columns, "columns", 15000, "number of JSON columns (a1..aN)")
	flag.IntVar(&cfg.Cases, "cases", 20, "number of payload cases")
	flag.IntVar(&cfg.StrLen, "str-len", 10, "string length for each column value")
	flag.IntVar(&cfg.QoS, "qos", 1, "MQTT QoS, only 1 is supported")
	flag.IntVar(&cfg.PhaseADurationSec, "phase-a-duration-secs", 60, "phase A duration seconds")
	flag.IntVar(&cfg.PhaseARateCrit, "phase-a-rate-critical", 120, "phase A critical publish rate")
	flag.IntVar(&cfg.PhaseARateBest, "phase-a-rate-best", 120, "phase A best publish rate")
	flag.IntVar(&cfg.PhaseBDurationSec, "phase-b-duration-secs", 60, "phase B duration seconds")
	flag.IntVar(&cfg.PhaseBRateCrit, "phase-b-rate-critical", 10, "phase B critical publish rate")
	flag.IntVar(&cfg.PhaseBRateBest, "phase-b-rate-best", 120, "phase B best publish rate")
	flag.StringVar(&cfg.ClientPrefix, "client-prefix", "perf-pr-go", "MQTT client id prefix")

	flag.Parse()
	if flag.NArg() > 0 {
		return nil, fmt.Errorf("unexpected positional args: %v", flag.Args())
	}
	return cfg, nil
}

func validateConfig(cfg *config) error {
	switch cfg.Phase {
	case "a", "b", "both":
	default:
		return fmt.Errorf("--phase must be one of: a | b | both")
	}

	if cfg.Columns <= 0 {
		return errors.New("--columns must be > 0")
	}
	if cfg.Cases <= 0 {
		return errors.New("--cases must be > 0")
	}
	if cfg.StrLen <= 0 {
		return errors.New("--str-len must be > 0")
	}
	if cfg.QoS != 1 {
		return errors.New("--qos must be 1")
	}
	if cfg.PhaseADurationSec < 0 || cfg.PhaseBDurationSec < 0 {
		return errors.New("phase duration must be >= 0")
	}
	if cfg.PhaseARateCrit < 0 || cfg.PhaseARateBest < 0 || cfg.PhaseBRateCrit < 0 || cfg.PhaseBRateBest < 0 {
		return errors.New("phase rates must be >= 0")
	}
	if strings.TrimSpace(cfg.TopicCritical) == "" || strings.TrimSpace(cfg.TopicBest) == "" {
		return errors.New("topic must not be empty")
	}
	if strings.TrimSpace(cfg.ClientPrefix) == "" {
		return errors.New("--client-prefix must not be empty")
	}

	u, err := url.Parse(cfg.BrokerURL)
	if err != nil {
		return fmt.Errorf("invalid --broker-url: %w", err)
	}
	if u.Scheme != "tcp" || u.Host == "" {
		return errors.New("--broker-url must be tcp://host:port")
	}
	return nil
}

func buildPhases(cfg *config) []phaseSettings {
	all := []phaseSettings{
		{
			Name:        "a",
			DurationSec: cfg.PhaseADurationSec,
			RateCrit:    cfg.PhaseARateCrit,
			RateBest:    cfg.PhaseARateBest,
		},
		{
			Name:        "b",
			DurationSec: cfg.PhaseBDurationSec,
			RateCrit:    cfg.PhaseBRateCrit,
			RateBest:    cfg.PhaseBRateBest,
		},
	}
	switch cfg.Phase {
	case "a":
		return all[:1]
	case "b":
		return all[1:]
	default:
		return all
	}
}

func generatePayloadCases(columnCount, cases, strLen int) ([][]byte, error) {
	keys := make([]string, columnCount)
	for idx := range keys {
		keys[idx] = fmt.Sprintf("a%d", idx+1)
	}

	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	out := make([][]byte, 0, cases)
	for i := 0; i < cases; i++ {
		obj := make(map[string]string, columnCount)
		for _, key := range keys {
			obj[key] = randomString(rng, strLen)
		}
		raw, err := json.Marshal(obj)
		if err != nil {
			return nil, fmt.Errorf("json marshal payload failed: %w", err)
		}
		out = append(out, raw)
	}
	return out, nil
}

func randomString(rng *rand.Rand, n int) string {
	buf := make([]byte, n)
	for i := range buf {
		buf[i] = randomAlphabet[rng.Intn(len(randomAlphabet))]
	}
	return string(buf)
}

func runPhase(cfg *config, phase phaseSettings, payloads [][]byte) error {
	start := time.Now().UTC()
	fmt.Printf(
		"[workload] phase=%s start=%s duration=%ds rate_critical=%d rate_best=%d\n",
		phase.Name,
		start.Format(time.RFC3339),
		phase.DurationSec,
		phase.RateCrit,
		phase.RateBest,
	)

	resultsCh := make(chan publisherResult, 2)
	duration := time.Duration(phase.DurationSec) * time.Second

	go publishLoad(
		cfg,
		"critical",
		cfg.TopicCritical,
		phase.RateCrit,
		duration,
		payloads,
		resultsCh,
	)
	go publishLoad(
		cfg,
		"best",
		cfg.TopicBest,
		phase.RateBest,
		duration,
		payloads,
		resultsCh,
	)

	results := make([]publisherResult, 0, 2)
	for i := 0; i < 2; i++ {
		results = append(results, <-resultsCh)
	}

	end := time.Now().UTC()
	fmt.Printf("[workload] phase=%s end=%s\n", phase.Name, end.Format(time.RFC3339))

	var finalErr error
	for _, result := range results {
		effectiveRate := 0.0
		if result.Duration > 0 {
			effectiveRate = float64(result.Sent) / result.Duration.Seconds()
		}
		fmt.Printf(
			"[workload] phase=%s publisher=%s topic=%s target_rate=%d sent=%d effective_rate=%.2f err=%v\n",
			phase.Name,
			result.Label,
			result.Topic,
			result.Rate,
			result.Sent,
			effectiveRate,
			result.Err,
		)
		if result.Err != nil && finalErr == nil {
			finalErr = fmt.Errorf("%s publisher failed: %w", result.Label, result.Err)
		}
	}
	return finalErr
}

func publishLoad(
	cfg *config,
	label string,
	topic string,
	ratePerSec int,
	duration time.Duration,
	payloads [][]byte,
	out chan<- publisherResult,
) {
	start := time.Now()
	res := publisherResult{
		Label: label,
		Topic: topic,
		Rate:  ratePerSec,
	}
	defer func() {
		res.Duration = time.Since(start)
		out <- res
	}()

	clientID := fmt.Sprintf("%s-%s-%d-%d", cfg.ClientPrefix, label, os.Getpid(), time.Now().UnixNano())
	opts := mqtt.NewClientOptions().
		AddBroker(cfg.BrokerURL).
		SetClientID(clientID).
		SetConnectTimeout(10 * time.Second).
		SetKeepAlive(60 * time.Second).
		SetAutoReconnect(false).
		SetCleanSession(true).
		SetOrderMatters(false)
	client := mqtt.NewClient(opts)

	connectToken := client.Connect()
	if !connectToken.WaitTimeout(15 * time.Second) {
		res.Err = errors.New("mqtt connect timeout")
		return
	}
	if connectToken.Error() != nil {
		res.Err = connectToken.Error()
		return
	}
	defer client.Disconnect(250)

	if duration <= 0 || ratePerSec <= 0 {
		if duration > 0 {
			time.Sleep(duration)
		}
		return
	}

	interval := time.Second / time.Duration(ratePerSec)
	deadline := time.Now().Add(duration)
	nextSend := time.Now()
	idx := 0
	qos := byte(cfg.QoS)

	for time.Now().Before(deadline) {
		payload := payloads[idx%len(payloads)]
		idx++
		token := client.Publish(topic, qos, false, payload)
		if !token.WaitTimeout(30 * time.Second) {
			res.Err = errors.New("mqtt publish timeout")
			return
		}
		if token.Error() != nil {
			res.Err = token.Error()
			return
		}
		res.Sent++

		nextSend = nextSend.Add(interval)
		sleepFor := time.Until(nextSend)
		if sleepFor > 0 {
			time.Sleep(sleepFor)
			continue
		}
		nextSend = time.Now()
	}
}
