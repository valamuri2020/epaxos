package configuration

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
)

type ID = string

var config Config

type Bconfig struct {
	T                    int     // total number of running time in seconds
	N                    int     // total number of requests
	K                    int     // key sapce
	W                    float64 // write ratio
	Throttle             int     // requests per second throttle, unused if 0
	Concurrency          int     // number of simulated clients
	Distribution         string  // distribution
	LinearizabilityCheck bool    // run linearizability checker at the end of benchmark
	// rounds       int    // repeat in many rounds sequentially

	// conflict distribution
	Conflicts int // percentage of conflicting keys
	Min       int // min key

	// normal distribution
	Mu    float64 // mu of normal distribution
	Sigma float64 // sigma of normal distribution
	Move  bool    // moving average (mu) of normal distribution
	Speed int     // moving speed in milliseconds intervals per key

	// zipfian distribution
	ZipfianS     float64 // zipfian s parameter
	ZipfianV     float64 // zipfian v parameter
	ZipfianTheta float64 // zipfian alpha = 1 / (1 - theta)

	// exponential distribution
	Lambda      float64 // rate parameter
	DumpLatency bool
}

type Config struct {
	Addrs      map[ID]string `json:"address"`
	Benchmark  Bconfig       `json:"benchmark"`
	BatchSize  int           `json:"batch_size"`
	BufferSize int           `json:"buffer_size"` // buffer size for maps

	N int
}

func (config *Config) Load() error {

	configFile, _ := os.Open("config.json")
	byteValue, _ := ioutil.ReadAll(configFile)
	err := json.Unmarshal(byteValue, &config)

	for range config.Addrs {
		config.N++
	}
	return err

}

func (config *Config) Print() {
	json_encoding, _ := json.Marshal(config)
	fmt.Println(string(json_encoding))
}

func GetConfig() Config {
	config.Load()
	return config
}
