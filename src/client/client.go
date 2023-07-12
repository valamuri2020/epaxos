package main

import (
	"bufio"
	"bytes"
	"configuration"
	"dlog"
	"encoding/gob"
	"flag"
	"fmt"
	"genericsmrproto"
	"kvproto"
	"log"
	"math/rand"
	"net"
	"os"
	"sort"
	"state"
	"sync"
	"time"
	"util"
)

var serverId = flag.Int("id", 0, "Id of server")
var startRange = flag.Int("sr", 0, "Key range start")
var sport = flag.Int("sport", 7074, "the port of the server")
var separate = flag.Bool("sp", false, "each batch contain one opeartion type")
var algo = flag.String("algo", "epaxos", "algorithm")

var INV = uint8(0)
var RES = uint8(1)
var config configuration.Config

func init() {
	config = configuration.GetConfig()
	flag.Parse()
	dlog.Setup(*serverId)
}

type Summary struct {
	AckNum     int
	TotalLat   int64
	LatArray   []int64
	MaxLat     int64
	TotalQueue int64
	TotalPrc   int64
	TotalExec  int64
	TotalSlow  int64
	TotalRead  int64
}

type OperationLog struct {
	Type uint8
	Op   kvproto.Operation
	K    kvproto.Key
	V    kvproto.Value
	Ts   int64
}

func main() {
	b := config.Benchmark
	conflicts := b.Conflicts
	readRatio := 1 - b.W
	reqNum := b.Throttle
	batchSize := config.BatchSize
	concurrency := b.Concurrency

	if conflicts > 100 {
		log.Fatalf("Conflicts percentage must be between 0 and 100.\n")
	}

	//generating keys
	tsArray := make([]int64, reqNum)
	ackTsArray := make([]int64, reqNum)
	//is it a read
	readArray := make([]bool, reqNum)
	kArray := make([]int64, reqNum)

	log.Printf("Zipfan Theta %f\n, ReadRatio: %f, Con: %d", b.ZipfianS, readRatio, concurrency)
	// log.Printf("Config %v", config)

	// zipGenerator := util.NewZipfianWithItems(int64(b.K), b.ZipfianTheta)
	// log.Printf("KeySpaace %d", int64(b.K))
	zipGenerator := util.NewZipfianWithItems(int64(b.K), b.ZipfianTheta)
	// seed := rand.New(rand.NewSource(time.Now().UTC().UnixNano()))
	// seed := rand.New(rand.NewSource(int64(*serverId)))
	seed := rand.New(rand.NewSource(int64(*serverId)))
	for i := 0; i < reqNum; i++ {
		if b.Distribution == "zipfan" {
			kArray[i] = zipGenerator.Next(seed)
			// kArray[i] = 42
			// kArray[i] = int64(*serverId*1000000 + i)
			// log.Printf("Key is %d", kArray[i])
		} else {
			r := rand.Intn(100)
			if r < conflicts {
				kArray[i] = 42
			} else {
				//we don't care about the conflict rate send to the same leader
				kArray[i] = int64(*startRange + 43 + i)
			}
		}
		tsArray[i] = 0
		ackTsArray[i] = 0
	}

	log.Printf("Client %d: KeyArray is %v\n", *serverId, kArray)

	for i := 0; i < reqNum; {
		bNum := min(batchSize, reqNum-i)
		isRead := false
		if rand.Float64() < readRatio {
			isRead = true
		}
		if *separate {
			for j := 0; j < bNum; j++ {
				readArray[i] = isRead
				if isRead {
				}
				i++
			}
		} else {
			readArray[i] = isRead
			i++
		}
	}

	outfilelock := &sync.Mutex{}
	outFileName := "./linearizability.out"
	f, _ := os.Create(outFileName)
	defer f.Close()
	w := bufio.NewWriter(f)
	//write to linearizabilty out
	defer w.Flush()
	server, err := net.Dial("tcp", fmt.Sprintf(":%d", *sport))
	if err != nil {
		log.Printf("Error connecting to replica %d at %v. Error is: %v \n", *serverId, fmt.Sprintf("127.0.0.1:%d", *sport), err)
	}
	reader := bufio.NewReader(server)
	writer := bufio.NewWriter(server)
	// inFlight := make(chan bool, (reqNum/batchSize)+1)
	totalCount := 0
	totalLatency := int64(0)
	var respSummary Summary

	if *algo == "abd" {
		inFlight := make(chan bool, concurrency)
		done := make(chan Summary)
		go abdClient(writer, 0, kArray, readArray, reqNum, inFlight, outfilelock, w)
		go getAbdResponse(reader, done, inFlight)
		respSummary = <-done
		totalCount = respSummary.AckNum
		totalLatency = respSummary.TotalLat
	} else { //epaxos
		inFlight := make(chan int, concurrency)
		done := make(chan Summary)
		go epaxosClient(writer, kArray, readArray, reqNum, inFlight, outfilelock, w)
		go readFastEpaxosResponse(reader, inFlight, done)
		respSummary = <-done
		totalCount = respSummary.AckNum
		totalLatency = respSummary.TotalLat
		//epaxos does not differentiate reand or write
		respSummary.TotalRead = int64(totalCount)
	}

	log.Printf("Throughput: %d req/s", totalCount/b.T)
	log.Printf("Lat per req: %d ms\n", totalLatency/int64(totalCount))
	log.Printf("Total latency: %d ms\n", totalLatency)
	log.Printf("Total slow %d\n", respSummary.TotalSlow)
	log.Printf("Slow rate %f\n", float64(respSummary.TotalSlow)/float64(respSummary.TotalRead))
	stat := Statistic(respSummary.LatArray[:totalCount])
	log.Println(stat)

	dlog.Infof("Throughput: %d req/s", totalCount/b.T)
	dlog.Infof("Lat per req: %d ms\n", totalLatency/int64(totalCount))
	dlog.Infof("Total latency: %d ms\n", totalLatency)
	dlog.Infof("Total slow %d\n", respSummary.TotalSlow)
	dlog.Infof("Slow rate %f\n", float64(respSummary.TotalSlow)/float64(respSummary.TotalRead))
	dlog.Info(stat)

	if b.DumpLatency {
		stat.WriteFile("latency." + fmt.Sprint(*serverId) + ".out")
	}

	server.Close()
}

func min(x, y int) int {
	if x > y {
		return y
	}
	return x
}

// send a single object
func sendObject(writer *bufio.Writer, object interface{}) {
	var bbuf bytes.Buffer
	var byteMsg []byte
	encoder := gob.NewEncoder(&bbuf)
	encoder.Encode(object)
	byteMsg = bbuf.Bytes()
	writer.Write(byteMsg)
	writer.Flush()
}

func abdClient(
	writer *bufio.Writer,
	clientId int,
	kArray []int64,
	rArray []bool,
	txnNum int,
	inFlight chan bool,
	fileLock *sync.Mutex,
	fileWriter *bufio.Writer) {
	time.Sleep(time.Duration(*serverId) * time.Millisecond)
	batchSize := config.BatchSize
	cmd := kvproto.Command{Op: kvproto.PUT, K: 0, Val: 0}
	n := txnNum //per client txn number
	batchNum := (n / batchSize) + 1
	log.Printf("Total req num is %d\n", n)
	benchTime := config.Benchmark.T
	batchInterval := time.Duration(config.Benchmark.T * 1e9 / batchNum)
	ticker := time.NewTicker(batchInterval)
	timer := time.NewTimer(time.Duration(benchTime) * time.Second)
	i := 0
loop:
	for i < n {
		select {
		case <-timer.C:
			break loop
		default:
			//construct transaction
			bNum := min(batchSize, n-i)
			var txn kvproto.Transaction
			for j := 0; j < bNum; j++ {
				cmd.K = kvproto.Key(kArray[clientId*txnNum+i])
				if rArray[i] {
					cmd.Op = kvproto.GET
				} else {
					cmd.Op = kvproto.PUT
					cmd.Val = kvproto.Value(rand.Int63n(10000000))
				}

				txn.Commands = append(txn.Commands, cmd)
				i++
			}

			if rArray[i-1] {
				txn.ReadOnly = 1
			} else {
				txn.ReadOnly = 0
			}

			//Hack: to avoid deadlock
			sort.Slice(txn.Commands, func(i, j int) bool {
				return txn.Commands[i].K < txn.Commands[j].K
			})

			<-ticker.C
			txn.Ts = util.MakeTimestamp(0)
			txn.TID = int64(i)
			sendObject(writer, txn)
			inFlight <- true
		}
	}
	log.Printf("Out of loop %d\n", i)
}

func getAbdResponse(reader *bufio.Reader, done chan Summary, inFlight chan bool) {
	var summary Summary //result summary
	summary.LatArray = make([]int64, config.Benchmark.Throttle)
	timer := time.NewTimer(time.Duration(config.Benchmark.T) * time.Second)
	idx := 0
	batchSize := config.BatchSize
	// reqNum := config.Benchmark.Throttle
	// batchNum := reqNum/batchSize + 1
	respMap := make(map[int64]int)
	respChan := make(chan kvproto.Response, config.BufferSize)

	go func() {
		for {
			gobReader := gob.NewDecoder(reader)
			var resp kvproto.Response
			if err := gobReader.Decode(&resp); err != nil {
				// log.Println("Error when reading:", err)
				continue
			}
			respChan <- resp
		}
	}()

loop:
	for {
		select {
		case <-timer.C:
			break loop
		case resp := <-respChan:
			tsNow := util.MakeTimestamp(0)
			summary.AckNum += resp.Size
			respMap[resp.TID] += resp.Size
			for i := 0; i < resp.Size; i++ {
				summary.LatArray[idx] = tsNow - resp.Ts
				idx++
			}
			summary.TotalLat += (tsNow - resp.Ts) * int64(resp.Size)
			if respMap[resp.TID] == batchSize {
				//complte a btach
				// log.Print("Complete a batch\n")
				<-inFlight
			}

			if len(resp.Vals) > 0 {
				summary.TotalRead += int64(resp.Size)
				if resp.IsFast == uint8(0) {
					//is slow response
					summary.TotalSlow += int64(resp.Size)
				}
			}

		}
	}
	log.Print("Client done\n")
	done <- summary
}

func epaxosClient(writer *bufio.Writer,
	kArray []int64, rArray []bool, txnNum int, inFlight chan int,
	fileLock *sync.Mutex, fileWriter *bufio.Writer) {
	time.Sleep(time.Duration(*serverId) * time.Millisecond)
	batchSize := config.BatchSize
	log.Printf("Total req num is %d\n", txnNum)
	var dummyValue state.Value
	testStart := time.Now()
	benchTime := config.Benchmark.T
	batchNum := (txnNum / batchSize) + 1
	batchInterval := time.Duration(config.Benchmark.T * 1e9 / batchNum)
	ticker := time.NewTicker(batchInterval)
	i := 0
	for i < txnNum {
		args := genericsmrproto.Propose{
			CommandId: 0,
			Command: state.Command{
				Op: state.PUT,
				K:  0,
				V:  dummyValue},
		}
		now := time.Now()
		if (now.Sub(testStart)).Seconds() > float64(benchTime) {
			break //terminate test
		}
		//construct send
		//write in a batch
		bNum := min(batchSize, txnNum-i)
		//i_start := i
		timeInt64 := util.MakeTimestamp(0)
		for j := 0; j < bNum; j++ {
			args.Timestamp = timeInt64
			args.Command.K = state.Key(kArray[i])
			if rArray[i] {
				args.Command.Op = state.GET
			} else {
				args.Command.Op = state.PUT
				args.Command.V = state.Value(rand.Int63n(10000000))
			}
			writer.WriteByte(genericsmrproto.PROPOSE)
			args.Marshal(writer)
			i++
		}
		inFlight <- bNum
		writer.Flush()
		<-ticker.C
	}
	log.Printf("Out of loop3 %d, interval %d \n", i, batchInterval)
}

func readFastEpaxosResponse(
	reader *bufio.Reader,
	inFlight chan int,
	done chan Summary) {
	var summary Summary //result summary
	summary.LatArray = make([]int64, config.Benchmark.Throttle)
	benchTime := config.Benchmark.T
	reply := new(genericsmrproto.ProposeReply)
	timer := time.NewTimer(time.Duration(benchTime) * time.Second)
	idx := 0

loop:
	for {
		select {
		case bSize := <-inFlight:
			for i := 0; i < bSize; i++ {
				if err := reply.Unmarshal(reader); err != nil {
					log.Println("Error when reading:", err)
					continue
				}
				timeInt64 := util.MakeTimestamp(0)
				lat := timeInt64 - reply.Timestamp
				// summary.TotalSlow += int64(reply.Slow)
				summary.AckNum += 1
				summary.LatArray[idx] = lat
				summary.TotalLat += lat
				idx++
			}
		case <-timer.C:
			break loop
		}
	}
	done <- summary
	log.Printf("Out of loop2\n")
}
