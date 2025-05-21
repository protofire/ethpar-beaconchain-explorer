package main

import (
	"errors"
	"flag"
	"net/http"

	"github.com/sirupsen/logrus"

	"github.com/protofire/ethpar-beaconchain-explorer/db2"
	"github.com/protofire/ethpar-beaconchain-explorer/db2/store"
	"github.com/protofire/ethpar-beaconchain-explorer/types"
	"github.com/protofire/ethpar-beaconchain-explorer/utils"
)

func main() {
	configPath := flag.String("config", "config/default.config.yml", "Path to the config file")
	flag.Parse()

	cfg := &types.Config{}
	err := utils.ReadConfig(cfg, *configPath)
	if err != nil {
		panic(err)
	}

	bt, err := store.NewBigTable(cfg.RawBigtable.Bigtable.Project, cfg.RawBigtable.Bigtable.Instance, nil)
	if err != nil {
		panic(err)
	}
	remote := store.NewRemoteStore(store.Wrap(bt, db2.BlocksRawTable, ""))
	go func() {
		logrus.Info("starting remote raw store on port 8087")
		if err := http.ListenAndServe("0.0.0.0:8087", remote.Routes()); err != nil && !errors.Is(err, http.ErrServerClosed) {
			panic(err)
		}
	}()
	utils.WaitForCtrlC()
}
