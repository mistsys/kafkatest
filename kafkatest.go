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
	"runtime"
	"strings"
	"sync"
	"time"
)

var log = stdlog.New(os.Stderr, "LOG> ", stdlog.Lshortfile|stdlog.Lmicroseconds)

// the URL of the kafka server to which to connect (any should do if they are up)
var KAFKA_BROKER = flag.String("kafka", "kafka-000-staging.mistsys.net:6667", "KAFKA_BROKER the <hostname>:<port number> of one of the kafka brokers")
var KAFKA_TOPIC = flag.String("topic", "user_profile_test", "KAFKA_TOPIC the kafka topic to which to publish and subscribe")
var NUM_ITERATIONS = flag.Int("iterations", 100, "NUM_ITERATIONS number of messages to publish to each partition")

var NUM_SKIP = flag.Int("skip", 0, "NUM_SKIP number of messages to skip before accumlulating statistics")
var NO_RESPONSE = flag.Bool("no-response", false, "kafka publisher doesn't wait for reponses")
var FLUSH_MSG_COUNT = flag.Int("flush-msg-count", 0, "this many messages queued up triggers a flush in the kafka publisher")
var CHANNEL_BUFFER_SIZE = flag.Int("channel-buffer-size", 256, "kafka go channel size")
var PAUSE_BETWEEN_MSGS = flag.Int("pause", 0, "# of msgs between which the publisher pauses very briefly")
var MSG_RATE = flag.Float64("rate", 0, "msgs/sec rate at which to send messages (not useful for very high rates, but can go below 1.0 if desired)")
var MSG_RTT = flag.Bool("rtt", false, "send next message as soon as the previous message arrives (only one message outstanding at a time)")

var MAX_WAIT_TIME = flag.Int("max-wait-time", 1, "kafka consumer max-wait-time (in msec)")
var MIN_FETCH_SIZE = flag.Int("min-fetch-size", 1, "kafka consumer min data requested")

var CONSUME_ONLY = flag.Bool("consume-only", false, "only run the kafka consumer half")
var PUBLISH_ONLY = flag.Bool("publish-only", false, "only run the kafka publisher half")

// time at which we started publishing (or the time of the 1st received message if we are just a consumer)
var pub_start time.Time

// channel used to signal that the receiver has consumer a message (used with MSG_RTT)
var rtt_wait = make(chan struct{}, 1)

func main() {
	flag.Parse()

	//runtime.GOMAXPROCS(runtime.NumCPU())
	log.Printf("GOMAXPROCS = %d\n", runtime.GOMAXPROCS(0))

	start := time.Now()

	// MSG_RTT without being both producer and consumer makes no sense
	if *MSG_RTT && (*CONSUME_ONLY || *PUBLISH_ONLY) {
		fmt.Printf("ERROR: -rtt without also being both producer and consumer makes no sense")
		return
	}

	// no URL means no kafka client
	if KAFKA_BROKER == nil || *KAFKA_BROKER == "" {
		fmt.Printf("ERROR: kafka broker URL must be specified\n")
		return
	}

	// enable logging to the general apsim logger, for debug
	sarama.Logger = log

	// start a kafka client and listen to the topic
	conf := sarama.NewConfig()
	conf.ClientID = "kafkatest"
	conf.Consumer.MaxWaitTime = time.Duration(*MAX_WAIT_TIME) * time.Millisecond // can't go below 1 msec
	conf.Consumer.Fetch.Min = int32(*MIN_FETCH_SIZE)
	if *NO_RESPONSE {
		conf.Producer.RequiredAcks = sarama.NoResponse // enabling NoResponse also has the side effect of making the publisher send each message to the TCP socket immediately
	}
	conf.Producer.Flush.Messages = *FLUSH_MSG_COUNT
	conf.Producer.Partitioner = sarama.NewRoundRobinPartitioner
	conf.ChannelBufferSize = *CHANNEL_BUFFER_SIZE
	fmt.Printf("Kafka client's Config %+v\n", conf)

	cl, err := sarama.NewClient([]string{*KAFKA_BROKER}, conf)
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
		offset, err := cl.GetOffset(*KAFKA_TOPIC, p, sarama.OffsetNewest)
		if err != nil {
			fmt.Printf("ERROR reading offset for partition %d of %q: %s\n", p, *KAFKA_TOPIC, err)
			return
		}
		fmt.Printf("%q partition %d LastOffset = %d\n", *KAFKA_TOPIC, p, offset)
		offsets[p] = offset
	}

	// and kick off readers for each partition
	var wg sync.WaitGroup
	var ready sync.WaitGroup
	var m Measurements
	m.Init("ms", 1)

	if !*PUBLISH_ONLY {
		con, err := sarama.NewConsumerFromClient(cl)
		if err != nil {
			fmt.Printf("ERROR creating consumer: %s\n", err)
			return
		}
		for _, p := range partitions {
			wg.Add(1)
			ready.Add(1)
			go read_partition(con, p, offsets[p], &m, &wg, &ready)
		}

		// wait for the consumers time to get connected and ready before we start publishing, otherwise we get the startup time included in the early measurements
		ready.Wait()
	}

	if !*CONSUME_ONLY {
		// kick off a publisher to the topic
		// (actually we just do it inline)
		wg.Add(1)
		pub_start = time.Now()
		publish(cl, len(partitions), &wg)
		pub_dur := time.Since(pub_start)
		log.Println("done publishing in", pub_dur, ",", float64(*NUM_ITERATIONS*len(partitions))/pub_dur.Seconds(), "msgs/sec")
	}

	wg.Wait()

	if !*PUBLISH_ONLY {
		con_dur := time.Since(pub_start)
		log.Println("done consuming in", con_dur, ",", float64(*NUM_ITERATIONS*len(partitions))/con_dur.Seconds(), "msgs/sec")

		fmt.Println(&m)
		fmt.Printf("test ran %f ms and published and received %d messages\n", time.Since(start).Seconds()*1000, *NUM_ITERATIONS*len(partitions))
	} else {
		fmt.Printf("test ran %f ms and published %d messages\n", time.Since(start).Seconds()*1000, *NUM_ITERATIONS*len(partitions))
	}
}

func publish(cl sarama.Client, num_partitions int, wg *sync.WaitGroup) {
	defer wg.Done()

	prod, err := sarama.NewAsyncProducerFromClient(cl)
	if err != nil {
		fmt.Printf("ERROR creating kafka producer to %q: %s", *KAFKA_TOPIC, err)
		return
	}
	defer prod.Close()
	input := prod.Input()
	errs := prod.Errors()

	// send messages, each containing the local nsec timestamp
	N := uint64(*NUM_ITERATIONS) * uint64(num_partitions)
	P := *PAUSE_BETWEEN_MSGS

	R := *MSG_RATE
	var T <-chan time.Time
	if R != 0 {
		interval := time.Duration(float64(time.Second) / R)
		T = time.Tick(interval)
	} else {
		t := make(chan time.Time)
		close(t)
		T = t
	}

	RTT := *MSG_RTT
	var W <-chan struct{}
	if RTT {
		W = rtt_wait
	} else {
		w := make(chan struct{})
		close(w)
		W = w
	}

	var i uint64
	var j = 1
	for i = 0; i < N; i++ {
		value := make([]byte, 16)
		binary.BigEndian.PutUint64(value, uint64(time.Now().UnixNano()))
		binary.BigEndian.PutUint64(value[8:], i) // out of curiosity, to more easily pick out which messages are which in an strace or pktcap
		value[8] = byte('n')                     // same, use a uniqish string to ID the messages
		value[9] = byte('s')
		value[10] = byte('d')
		msg := sarama.ProducerMessage{Topic: *KAFKA_TOPIC, Value: sarama.ByteEncoder(value)}
		select {
		case input <- &msg:
			// great
		case err := <-errs:
			fmt.Printf("ERROR publishing to kafka: %s", err)
			return
		}

		// pause between messages (or, if no rate was specified, then T is an already closed channel and this does nothing very quickly)
		<-T

		// pause waiting for the rtt (or, it no rate was specified, then W is already closed)
		<-W

		if j == P {
			time.Sleep(time.Nanosecond) // NOTE the actual delay is rounded way up by the runtime and kernel
			//runtime.Gosched() // yielding to the go scheduler has no effect
			j = 0
		}
		j++
	}
}

func read_partition(con sarama.Consumer, partition int32, offset int64, m *Measurements, wg *sync.WaitGroup, ready *sync.WaitGroup) {
	defer wg.Done()
	pcon, err := con.ConsumePartition(*KAFKA_TOPIC, partition, sarama.OffsetNewest)
	if err != nil {
		fmt.Printf("ERROR creating kafka subscription to %q: %s", *KAFKA_TOPIC, err)
		return
	}
	defer pcon.Close()

	// now loop receiving messages from this kafka/partition
	N := *NUM_ITERATIONS
	S := *NUM_SKIP
	ready.Done()
	for i := 0; i < N; i++ {
		ev := <-pcon.Messages()
		now := time.Now()
		// print in one call to write() so that we don't interleave with writes from other partitions
		//os.Stdout.WriteString(fmt.Sprintf("ev.Key = %T %s\n%v\n"+ "ev.Value = %T %s\n%v\n", ev.Key, ev.Key, ev.Key, ev.Value, ev.Value, ev.Value))
		delta := now.UnixNano() - int64(binary.BigEndian.Uint64(ev.Value))
		if i >= S {
			m.Accumulate(float64(delta) / 1000000)
		}
		if i == 0 && *CONSUME_ONLY && partition == 0 {
			// use the time in the 1st message as the best guess at the start time
			pub_start = now.Add(-time.Duration(delta) * time.Nanosecond)
			fmt.Println("started receiving msgs")
		}

		// tell the publisher we're ready, if it cares
		select {
		case rtt_wait <- struct{}{}:
		default:
		}
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
}

func (m *Measurements) Init(units string, binsize float64) {
	m.units = units
	m.binsize = binsize
	m.min = math.MaxFloat64
	m.max = -math.MaxFloat64

	if m.binsize != 0 {
		m.counts = make(map[int64]uint64)
	}
}

// add sample x to m
func (m *Measurements) Accumulate(x float64) {
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

	if m.binsize != 0 {
		b := int64(math.Floor((x / m.binsize) + 0.5)) // really I would want an arbitrary large int, but this will do for my data ranges
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
			var c uint64
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

		var accu uint64
		for b := first_bin; b < last_bin; b += bins_per_row {
			x := (float64(b) + float64(bins_per_row-1)/2) * x_scale
			var c uint64
			for o := int64(0); o < bins_per_row; o++ {
				c += m.counts[b+o]
			}
			accu += c
			percent := 100 * float64(accu) / float64(m.n)
			cc := int(math.Ceil(float64(c) * c_scale))
			s := fmt.Sprintf("%6.1f %5.1f%% %6d: %s", x, percent, c, strings.Repeat("*", cc))
			sa = append(sa, s)
		}
	}

	return strings.Join(sa, "\n")
}
