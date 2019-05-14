package main

import (
	log "github.com/sirupsen/logrus"
	"github.com/vladkampov/url-shortener/api"
	"github.com/vladkampov/url-shortener/db"
	"os"
)

func main() {
	log.Println("We are about to go...")
	dbSession := db.InitCassandra()

	defer dbSession.Close()
	err := api.Run()
	if err != nil {
		log.Errorf("API error: %s", err)
		os.Exit(1)
		return
	}
}
