# kafkatest golang kafka performance

Kafak performance test in Go

Round-trip time and performance tests for kafka
using the Shaopfy/sarama Go kafaka client.

Example uses:
  * Show all the options and their default values:

````
        kafkatest -h

        Usage of kafkatest:
          -channel-buffer-size=0: kafka publisher go channel size
          -consume-only=false: only run the kafka consumer half
          -event-buffer-size=16: kafka consumer go channel size
          -flush-msg-count=0: this many messages queued up triggers a flush in the kafka publisher
          -iterations=100: NUM_ITERATIONS number of messages to publish to each partition
          -kafka="my-kafka-server:6667": KAFKA_BROKER the <hostname>:<port number> of one of the kafka brokers
          -max-wait-time=1: kafka consumer max-wait-time (in msec)
          -min-fetch-size=1: kafka consumer min data requested
          -no-response=false: kafka publisher doesn't wait for reponses
          -pause=0: # of msgs between which the publisher pauses very briefly
          -publish-only=false: only run the kafka publisher half
          -rate=0: msgs/sec rate at which to send messages (not useful for very high rates, but can go below 1.0 if desired)
          -rtt=false: send next message as soon as the previous message arrives (only one message outstanding at a time)
          -skip=0: NUM_SKIP number of messages to skip before accumlulating statistics
          -topic="user_profile_test": KAFKA_TOPIC the kafka topic to which to publish and subscribe
````

  * Test the RTT of publishing->queue in kafka->consuming.

````
      kafkatest -kafka ... -topic ... -rtt
````
      
  Output looks like
      
````
        ndade@mist-004:~/go/src/github.com/mistsys/kafkatest$ kafkatest -kafka my-kafka-server.com:6667 -topic my_test_topic -rtt
        LOG> 14:34:32.661126 kafkatest.go:56: GOMAXPROCS = 1
        LOG> 14:34:32.661267 client.go:42: Initializing new client
        LOG> 14:34:32.661292 client.go:353: Fetching metadata from broker my-kafka-server.com:6667
        LOG> 14:34:32.786714 broker.go:113: Connected to broker my-kafka-server.com:6667
        LOG> 14:34:32.874886 client.go:521: Registered new broker #172311936 at my-kafka-server.com:6667
        LOG> 14:34:32.874935 client.go:82: Successfully initialized new client
        "user_profile_test" partitions = [0]
        LOG> 14:34:32.970241 broker.go:113: Connected to broker my-kafka-server.com:6667
        "user_profile_test" partition 0 LastOffset = 0
        ConsumerConfig &{DefaultFetchSize:32768 MinFetchSize:1 MaxMessageSize:0 MaxWaitTime:1ms OffsetMethod:1 OffsetValue:0 EventBufferSize:16}
        LOG> 14:34:33.059874 consumer.go:376: ConsumerConfig.MaxWaitTime is very low, which can cause high CPU and network usage. See documentation for details.
        ProducerConfig &{Partitioner:0x472df0 RequiredAcks:1 Timeout:0 Compression:0 FlushMsgCount:0 FlushFrequency:0 FlushByteCount:0 AckSuccesses:false MaxMessageBytes:1000000 MaxMessagesPerReq:0 ChannelBufferSize:0 RetryBackoff:250ms}
        LOG> 14:34:51.847548 producer.go:268: Producer shutting down.
        LOG> 14:34:51.847635 kafkatest.go:131: done publishing in 18.696904266s , 5.348479009000879 msgs/sec
        LOG> 14:34:51.939621 kafkatest.go:138: done consuming in 18.788887797s , 5.322294809593087 msgs/sec
        100 samples. Avg 186.955952 ms. Max 240.100378 ms. Min 179.400195 ms.
         179.5   6.0%      6: ******
         181.5  19.0%     13: *************
         183.5  45.0%     26: **************************
         185.5  71.0%     26: **************************
         187.5  76.0%      5: *****
         189.5  82.0%      6: ******
         191.5  86.0%      4: ****
         193.5  92.0%      6: ******
         195.5  93.0%      1: *
         197.5  94.0%      1: *
         199.5  94.0%      0: 
         201.5  95.0%      1: *
         203.5  96.0%      1: *
         205.5  97.0%      1: *
         207.5  98.0%      1: *
         209.5  99.0%      1: *
         211.5  99.0%      0: 
         213.5  99.0%      0: 
         215.5  99.0%      0: 
         217.5  99.0%      0: 
         219.5  99.0%      0: 
         221.5  99.0%      0: 
         223.5  99.0%      0: 
         225.5  99.0%      0: 
         227.5  99.0%      0: 
         229.5  99.0%      0: 
         231.5  99.0%      0: 
         233.5  99.0%      0: 
         235.5  99.0%      0: 
         237.5  99.0%      0: 
         239.5 100.0%      1: *
         241.5 100.0%      0: 
        test ran 19278.805828 ms and published and received 100 messages
        LOG> 14:34:51.940073 client.go:101: Closing Client
        ndade@mist-004:~/go/src/github.com/mistsys/kafkatest$ 
````

  * Test the throughput of publishing & consuming as fast as possible
  
````
        kafkatest -kafka ... -topic ... -iterations 10000 -rate 4000
````

  Output looks like

````
        ndade@mist-004:~/go/src/github.com/mistsys/kafkatest$ kafkatest -kafka my-kafka-server.com:6667 -topic my_test_topic -iterations 10000 -rate 4000
        LOG> 14:40:24.781869 kafkatest.go:56: GOMAXPROCS = 1
        LOG> 14:40:24.782063 client.go:42: Initializing new client
        LOG> 14:40:24.782093 client.go:353: Fetching metadata from broker my-kafka-server.com:6667
        LOG> 14:40:24.900577 broker.go:113: Connected to broker my-kafka-server.com:6667
        LOG> 14:40:24.993306 client.go:521: Registered new broker #172311936 at my-kafka-server.com:6667
        LOG> 14:40:24.993367 client.go:82: Successfully initialized new client
        "user_profile_test" partitions = [0]
        LOG> 14:40:25.089532 broker.go:113: Connected to broker my-kafka-server.com:6667
        "user_profile_test" partition 0 LastOffset = 127781
        ConsumerConfig &{DefaultFetchSize:32768 MinFetchSize:1 MaxMessageSize:0 MaxWaitTime:1ms OffsetMethod:1 OffsetValue:127781 EventBufferSize:16}
        LOG> 14:40:25.182061 consumer.go:376: ConsumerConfig.MaxWaitTime is very low, which can cause high CPU and network usage. See documentation for details.
        ProducerConfig &{Partitioner:0x472df0 RequiredAcks:1 Timeout:0 Compression:0 FlushMsgCount:0 FlushFrequency:0 FlushByteCount:0 AckSuccesses:false MaxMessageBytes:1000000 MaxMessagesPerReq:0 ChannelBufferSize:0 RetryBackoff:250ms}
        LOG> 14:40:27.861737 producer.go:268: Producer shutting down.
        LOG> 14:40:27.976231 kafkatest.go:131: done publishing in 2.70836438s , 3692.2653664496947 msgs/sec
        LOG> 14:40:28.070491 kafkatest.go:138: done consuming in 2.802645238s , 3568.0577278971327 msgs/sec
        10000 samples. Avg 237.615572 ms. Max 479.387664 ms. Min 104.908779 ms.
         107.0   0.6%     62: *********
         114.0   1.7%    112: ****************
         121.0   3.3%    158: ***********************
         128.0   5.0%    168: ************************
         135.0   6.7%    172: *************************
         142.0   8.4%    169: ************************
         149.0  10.2%    174: *************************
         156.0  12.8%    269: **************************************
         163.0  16.5%    366: ****************************************************
         170.0  20.3%    378: *****************************************************
         177.0  24.6%    432: *************************************************************
         184.0  29.2%    459: *****************************************************************
         191.0  34.2%    501: **********************************************************************
         198.0  38.8%    460: *****************************************************************
         205.0  43.7%    489: *********************************************************************
         212.0  47.9%    423: ************************************************************
         219.0  52.0%    409: **********************************************************
         226.0  56.3%    429: ************************************************************
         233.0  60.5%    423: ************************************************************
         240.0  64.7%    421: ***********************************************************
         247.0  68.7%    395: ********************************************************
         254.0  71.6%    289: *****************************************
         261.0  74.0%    246: ***********************************
         268.0  76.0%    196: ****************************
         275.0  77.3%    134: *******************
         282.0  78.2%     91: *************
         289.0  78.9%     62: *********
         296.0  79.7%     78: ***********
         303.0  80.8%    111: ****************
         310.0  81.9%    113: ****************
         317.0  83.0%    114: ****************
         324.0  84.2%    112: ****************
         331.0  85.3%    112: ****************
         338.0  86.4%    113: ****************
         345.0  87.5%    114: ****************
         352.0  88.7%    113: ****************
         359.0  89.8%    109: ****************
         366.0  90.6%     86: *************
         373.0  91.5%     84: ************
         380.0  92.2%     79: ************
         387.0  92.8%     57: ********
         394.0  93.4%     54: ********
         401.0  93.9%     56: ********
         408.0  94.5%     56: ********
         415.0  95.0%     57: ********
         422.0  95.6%     57: ********
         429.0  96.2%     53: ********
         436.0  96.7%     56: ********
         443.0  97.3%     58: *********
         450.0  97.8%     56: ********
         457.0  98.4%     59: *********
         464.0  99.0%     60: *********
         471.0  99.6%     56: ********
         478.0 100.0%     40: ******
        test ran 3289.630377 ms and published and received 10000 messages
        LOG> 14:40:28.071699 client.go:101: Closing Client
        ndade@mist-004:~/go/src/github.com/mistsys/kafkatest$ 
````
  
  
