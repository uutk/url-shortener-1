package main

import (
	log "github.com/sirupsen/logrus"
	"github.com/vladkampov/url-shortener/api"
	"github.com/vladkampov/url-shortener/db"
)

func main() {
	log.Println("We are about to go...")
	dbSession := db.InitCassandra()
	db.ReadURL("lbnBgP")
	defer dbSession.Close()
	api.Init()
}