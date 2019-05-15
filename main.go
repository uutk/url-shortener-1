package main

import (
	"github.com/joho/godotenv"
	log "github.com/sirupsen/logrus"
	"github.com/vladkampov/url-shortener/api"
	"github.com/vladkampov/url-shortener/db"
	"os"
)

func main() {
	err := godotenv.Load()
	if err != nil {
		log.Fatal("Error loading .env file")
	}

	log.Println("We are about to go...")
	dbSession, err := db.RunCassandra()
	if err != nil {
		log.Errorf("DB error: %s", err)
		os.Exit(1)
		return
	}
	defer dbSession.Close()

	err = api.Run()
	if err != nil {
		log.Errorf("API error: %s", err)
		os.Exit(1)
		return
	}
}
