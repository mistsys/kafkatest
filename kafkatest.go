/*
 * Test kafka performance. Publish and receive messages and keep track of the round trip time
 *
 * Copyright 2014 MistSys
 *
 */

package main

import (
	"encoding/binary"
	"flag"
	"fmt"
	"github.com/Shopify/sarama"
	stdlog "log"
	"math"
	"os"
	"sort"
	"strings"
	"sync"
	"time"
)

var log = stdlog.New(os.Stderr, "LOG> ", stdlog.Lshortfile|stdlog.Lmicroseconds)

// the URL of the kafka server to which to connect (any should do if they are up)
var KAFKA_BROKER = flag.String("kafka", "kafka-000-staging.mistsys.net:6667", "KAFKA_BROKER the <hostname>:<port number> of one of the kafka brokers")
var KAFKA_TOPIC = flag.String("topic", "user_profile_test", "KAFKA_TOPIC the kafka topic to which to publish and subscribe")
var NUM_ITERATIONS = flag.Int("iterations", 100, "NUM_ITERATIONS number of messages to publish to each partition")

func main() {
	flag.Parse()

	// no URL means no kafka client
	if KAFKA_BROKER == nil || *KAFKA_BROKER == "" {
		fmt.Printf("kafka broker URL must be specified\n")
		return
	}

	// enable logging to the general apsim logger, for debug
	sarama.Logger = log

	// start a kafka client and listen to the topic
	cl, err := sarama.NewClient("kafkatest", []string{*KAFKA_BROKER}, nil)
	if err != nil {
		fmt.Printf("ERROR creating kafka client to %q: %s", *KAFKA_BROKER, err)
		return
	}
	defer cl.Close()

	if KAFKA_TOPIC == nil || *KAFKA_TOPIC == "" {
		// no topic was specified.  show the available topics and exit
		fmt.Println("ERROR No -topic specified")
		return
	}

	// check the latest offsets for our topic
	partitions, err := cl.Partitions(*KAFKA_TOPIC)
	if err != nil {
		fmt.Printf("ERROR reading partitions for %q: %s\n", *KAFKA_TOPIC, err)
		return
	}
	fmt.Printf("%q partitions = %v\n", *KAFKA_TOPIC, partitions)
	offsets := make(map[int32]int64, len(partitions))
	for _, p := range partitions {
		offset, err := cl.GetOffset(*KAFKA_TOPIC, p, sarama.LatestOffsets)
		if err != nil {
			fmt.Printf("ERROR reading offset for partition %d of %q: %s\n", p, *KAFKA_TOPIC, err)
			return
		}
		fmt.Printf("%q partition %d LastOffset = %d\n", *KAFKA_TOPIC, p, offset)
		offsets[p] = offset
	}

	// and kick off readers for each partition
	var wg sync.WaitGroup
	var m Measurements
	m.Init("ms", 0.5)
	for _, p := range partitions {
		wg.Add(1)
		go read_partition(cl, p, offsets[p], &m, &wg)
	}

	// kick off a writer to the topic
	// (actually we just do it inline)
	wg.Add(1)
	publish(cl, len(partitions), &wg)

	wg.Wait()

	fmt.Println(&m)
}

func publish(cl *sarama.Client, num_partitions int, wg *sync.WaitGroup) {
	defer wg.Done()
	var prod_conf = sarama.NewProducerConfig()
	prod_conf.RequiredAcks = sarama.NoResponse
	prod_conf.Partitioner = sarama.NewRoundRobinPartitioner
	prod, err := sarama.NewProducer(cl, prod_conf)
	//prod, err := sarama.NewSimpleProducer(cl, *KAFKA_TOPIC, sarama.NewRoundRobinPartitioner)
	if err != nil {
		fmt.Printf("ERROR creating kafka producer to %q: %s", *KAFKA_TOPIC, err)
		return
	}
	input := prod.Input()

	// send messages, each containing the local nsec timestamp
	N := *NUM_ITERATIONS * num_partitions
	for i := 0; i < N; i++ {
		value := make([]byte, 8)
		binary.BigEndian.PutUint64(value, uint64(time.Now().UnixNano()))
		msg := sarama.MessageToSend{Topic: *KAFKA_TOPIC, Value: sarama.ByteEncoder(value)}
		input <- &msg
	}
}

func read_partition(cl *sarama.Client, partition int32, offset int64, m *Measurements, wg *sync.WaitGroup) {
	defer wg.Done()
	con_config := sarama.NewConsumerConfig()
	con_config.OffsetMethod = sarama.OffsetMethodNewest
	con_config.OffsetValue = offset // not really needed unless we use OffsetMethodManual
	con, err := sarama.NewConsumer(cl, *KAFKA_TOPIC, partition, "", con_config)
	if err != nil {
		fmt.Printf("ERROR creating kafka subscription to %q: %s", *KAFKA_TOPIC, err)
		return
	}
	defer con.Close()

	// now loop receiving messages from this kafka/partition
	N := *NUM_ITERATIONS
	for i := 0; i < N; i++ {
		ev := <-con.Events()
		now := time.Now()
		if ev.Err != nil {
			fmt.Printf("ERROR receiving kafka message: %s\n", ev.Err)
			continue
		}
		// print in one call to write() so that we don't interleave with writes from other partitions
		//os.Stdout.WriteString(fmt.Sprintf("ev.Key = %T %s\n%v\n"+ "ev.Value = %T %s\n%v\n", ev.Key, ev.Key, ev.Key, ev.Value, ev.Value, ev.Value))
		delta := now.UnixNano() - int64(binary.BigEndian.Uint64(ev.Value))
		m.Accumulate(float64(delta)/1000000, now)
	}
}

// accumulated measurements of some repeated experiment
type Measurements struct {
	lock sync.Mutex

	units   string           // the units of the measurement (used for pretty printing)
	binsize float64          // size of the bins used for accumulating counts
	counts  map[int64]uint64 // maps bin number to the count of how many x values fell into that bin

	n        int     // # of times x was accumulated
	sum      float64 // sum of all x
	min, max float64
	top10    []TopTen // top 10 slowest accesses
}

type TopTen struct {
	value float64   // the value of x
	when  time.Time // timestamp when max x happened
}

func (m *Measurements) Init(units string, binsize float64) {
	m.units = units
	m.binsize = binsize
	m.min = math.MaxFloat64
	m.max = -math.MaxFloat64

	if m.binsize != 0 {
		m.counts = make(map[int64]uint64)
	}

	m.top10 = make([]TopTen, 0, 10)
}

// add sample x to m
func (m *Measurements) Accumulate(x float64, when time.Time) {
	m.lock.Lock()
	defer m.lock.Unlock()

	m.n++
	m.sum += x
	if x < m.min {
		m.min = x
	}
	if x > m.max {
		m.max = x
	}
	topN := len(m.top10)
	if topN < cap(m.top10) || x > m.top10[topN-1].value {
		// insert x into the top10 array
		idx := sort.Search(topN, func(i int) bool { return x > m.top10[i].value })
		if topN < cap(m.top10) {
			m.top10 = append(m.top10, TopTen{})
		}
		copy(m.top10[idx+1:], m.top10[idx:])
		e := &m.top10[idx]
		e.value = x
		e.when = when
	}

	if m.binsize != 0 {
		b := int64(math.Floor(x / m.binsize)) // really I would want an arbitrary large int, but this will do for my data ranges
		m.counts[b]++
	} // else don't accumulate pop counts
}

func (m *Measurements) String() string {
	m.lock.Lock()
	defer m.lock.Unlock()

	sa := make([]string, 1)
	sa[0] = fmt.Sprintf("%d samples. Avg %f %s. Max %f %s. Min %f %s.", m.n, m.sum/float64(m.n), m.units, m.max, m.units, m.min, m.units)

	if len(m.counts) != 0 {
		// pretty print the population graph in crude ascii 80 columns wide
		// first figure out how many rows this will take
		first_bin := int64(math.Floor(m.min / m.binsize))
		last_bin := 1 + int64(math.Ceil(m.max/m.binsize)) // actually last bin + 1
		bins := last_bin - first_bin
		bins_per_row := int64(1)

		// TODO get size of TTY via termios
		if bins > 60 { // print at most 60 rows
			// we need to merge consecutive bins into a single row of output to avoid printing a huge number of output lines
			bins_per_row = int64(math.Ceil(float64(bins) / 60))
		}

		// and figure out the largest count
		var max_c uint64 = 0
		for b := first_bin; b < last_bin; b += bins_per_row {
			c := m.counts[b]
			for o := int64(0); o < bins_per_row; o++ {
				c += m.counts[b+o]
			}
			if max_c < c {
				max_c = c
			}
		}
		c_scale := float64(80-10) / float64(max_c) // use 80-10 = 70 columns
		if c_scale > 1 {
			c_scale = 1
		}

		x_first := float64(first_bin) * m.binsize
		x_last := float64(last_bin) * m.binsize
		x_scale := (x_last - x_first) / float64(bins)

		for b := first_bin; b < last_bin; b += bins_per_row {
			x := (float64(b) + float64(bins_per_row-1)/2) * x_scale
			c := m.counts[b]
			for o := int64(0); o < bins_per_row; o++ {
				c += m.counts[b+o]
			}
			cc := int(math.Ceil(float64(c) * c_scale))
			s := fmt.Sprintf("%6.1f %6d: %s", x, c, strings.Repeat("*", cc))
			sa = append(sa, s)
		}
	}

	if len(m.top10) != 0 {
		sa = append(sa, fmt.Sprintf("Top %d slowest :", len(m.top10)))
		for i, tt := range m.top10 {
			sa = append(sa, fmt.Sprintf(" %2d. %f %s at %s", i+1, tt.value, m.units, tt.when.UTC()))
		}
	}

	return strings.Join(sa, "\n")
}
