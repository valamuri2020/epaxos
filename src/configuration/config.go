package configuration

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"strings"
)

type ID = string

var config Config

type Config struct {
	Addrs map[ID]string `json:"address"`
	Ports map[ID]string
	N     int
}

func (config *Config) Load() error {

	configFile, _ := os.Open("config.json")
	byteValue, _ := ioutil.ReadAll(configFile)
	err := json.Unmarshal(byteValue, &config)

	config.Ports = make(map[ID]string)

	for key, addr := range config.Addrs {
		addr, port := strings.Split(addr, ":")[0], strings.Split(addr, ":")[1]
		config.Addrs[key] = addr
		config.Ports[key] = port
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
