package main

// finds this from where we start the server
import (
	"configuration"
	"epaxos"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"os/signal"
	"paxos"
	"runtime"
	"runtime/pprof"
	"strconv"
	"strings"
)

var id *string = flag.String("id", "0", "ID of replica")
var config *string = flag.String("config", "", "path to config file")
var masterAddr *string = flag.String("maddr", "", "Master address. Defaults to localhost.")
var masterPort *int = flag.Int("mport", 7087, "Master port.  Defaults to 7087.")
var doEpaxos *bool = flag.Bool("e", true, "Use EPaxos as the replication protocol. Defaults to false.")
var procs *int = flag.Int("p", 2, "GOMAXPROCS. Defaults to 2")
var cpuprofile = flag.String("cpuprofile", "", "write cpu profile to file")
var thrifty = flag.Bool("thrifty", false, "Use only as many messages as strictly required for inter-replica communication.")
var beacon = flag.Bool("beacon", false, "Send beacons to other replicas to compare their relative speeds.")
var durable = flag.Bool("durable", false, "Log to a stable store (i.e., a file in the current dir).")
var batch = flag.Bool("batch", false, "Enables batching of inter-server messages")
var infiniteFix = flag.Bool("inffix", false, "Enables a bound on execution latency for EPaxos")
var clockSyncType = flag.Int("clocksync", 0, "0 to not sync clocks, 1 to delay the opening of messages until the quorum, 2 to delay so that all process at same time, 3 to delay to CA, VA, and OR.")
var clockSyncEpsilon = flag.Float64("clockepsilon", 4, "The number of milliseconds to add as buffer for OpenAfter times.")

func main() {
	flag.Parse()
	config := configuration.GetConfig()

	ID := (configuration.ID)(*id)
	nodeList := []string{}
	for _, addr := range config.Addrs {
		nodeList = append(nodeList, addr)
	}

	portNum, _ := strconv.Atoi(strings.Split(config.Addrs[ID], ":")[1])
	replicaId, _ := strconv.Atoi(ID)
	runtime.GOMAXPROCS(*procs)

	if *cpuprofile != "" {
		f, err := os.Create(*cpuprofile)
		if err != nil {
			log.Fatal(err)
		}
		pprof.StartCPUProfile(f)

		interrupt := make(chan os.Signal, 1)
		signal.Notify(interrupt)
		go catchKill(interrupt)
	}

	log.Printf("Server starting on port %v\n", portNum)

	fmt.Println("ReplicaID: ", replicaId, "NodeList: ", nodeList)
	// Should be like ReplicaID:  1 NodeList:  [127.0.0.1:7074 127.0.0.1:7075 127.0.0.1:7076]
	if *doEpaxos {
		log.Println("Starting Egalitarian Paxos replica...")
		rep := epaxos.NewReplica(replicaId, nodeList, *thrifty, *beacon,
			*durable, *batch, *infiniteFix, epaxos.ClockSyncType(*clockSyncType),
			int64(*clockSyncEpsilon*1e6) /* ms to ns */)
		rpc.Register(rep)
	} else {
		log.Println("Starting classic Paxos replica...")
		rep := paxos.NewReplica(replicaId, nodeList, *thrifty, *durable, *batch)
		rpc.Register(rep)
	}

	rpc.HandleHTTP()
	//listen for RPC on a different port (8070 by default)

	l, err := net.Listen("tcp", fmt.Sprintf(":%d", portNum+1000))
	if err != nil {
		log.Fatal("listen error:", err)
	}

	http.Serve(l, nil)
}

func catchKill(interrupt chan os.Signal) {
	<-interrupt
	if *cpuprofile != "" {
		pprof.StopCPUProfile()
	}
	fmt.Println("Caught signal")
	os.Exit(0)
}
