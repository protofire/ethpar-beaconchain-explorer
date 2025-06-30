package main

import (
	"flag"
	"fmt"

	"github.com/protofire/ethpar-beaconchain-explorer/exporter"
	"github.com/protofire/ethpar-beaconchain-explorer/internal/metrics"
	"github.com/protofire/ethpar-beaconchain-explorer/types"
	"github.com/protofire/ethpar-beaconchain-explorer/utils"
	"github.com/protofire/ethpar-beaconchain-explorer/version"

	"github.com/sirupsen/logrus"
)

func main() {
	configFlag := flag.String("config", "config.yml", "path to config")
	versionFlag := flag.Bool("version", false, "print version and exit")
	flag.Parse()
	if *versionFlag {
		fmt.Println(version.Version)
		return
	}
	utils.Config = &types.Config{}
	err := utils.ReadConfig(utils.Config, *configFlag)
	if err != nil {
		logrus.Fatal(err)
	}
	metrics.StartMetrics(utils.Config.Metrics.Enabled, utils.Config.Metrics.Address)
	blobIndexer, err := exporter.NewBlobIndexer()
	if err != nil {
		logrus.Fatal(err)
	}
	go blobIndexer.Start()
	utils.WaitForCtrlC()
}
