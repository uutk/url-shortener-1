package db

import (
	"github.com/gocql/gocql"
	log "github.com/sirupsen/logrus"
	"github.com/vladkampov/url-shortener/helpers"
	"os"
	"time"
	"github.com/mitchellh/mapstructure"
)

type URL struct{
	Id gocql.UUID
	Url string
	Hash string
	Ts time.Time
	Visited int32
	UserId string
}

var s *gocql.Session

func ReadURLsByUserId(userId string) ([]URL, error) {
	var urls []URL

	iter := s.Query(`SELECT id, url, hash, ts, visited FROM url_shortener.urls WHERE user_id = ? ALLOW FILTERING`,
		userId).Consistency(gocql.All).Iter()

	s, err := iter.SliceMap()

	if err != nil {
		return nil, err
	}

	for _, item := range s {
		var url URL
		err := mapstructure.Decode(item, &url)

		if err != nil {
			return nil, err
		}

		urls = append(urls, url)
	}

	return urls, nil
}

func WriteURL(url string, userId string) (string, error) {
	id := gocql.TimeUUID()
	hash := helpers.GetRandomString(6)

	// return url if it was already shortened for current user
	var hashFromDB string
	err := s.Query(`SELECT hash FROM url_shortener.urls WHERE url = ? AND user_id = ? LIMIT 1 ALLOW FILTERING`,
		url, userId).Consistency(gocql.One).Scan(&hashFromDB)

	if  err == nil {
		log.Println(hashFromDB)
		return hashFromDB, nil
	}

	// Recreate hash if there's an entity with same hash
	for true {
		if err := s.Query(`SELECT id FROM url_shortener.urls WHERE hash = ? LIMIT 1 ALLOW FILTERING`,
			hash).Consistency(gocql.One); err != nil {
			id = gocql.TimeUUID()
			break
		}
	}

	log.Println("lalka")

	if err := s.Query(`INSERT INTO url_shortener."urls" (id, url, hash, ts, visited, user_id) VALUES (?, ?, ?, ?, ?, ?)`,
		id, url, hash, time.Now(), 0, userId).Exec(); err != nil {
		return "", err
	}

	return hash, nil
}

func ReadURL(hash string) (URL, error) {
	var id gocql.UUID
	var url string
	var ts time.Time
	var visited int32
	var userId string
	var urlStruct URL

	if err := s.Query(`SELECT id, url, ts, visited, user_id FROM url_shortener.urls WHERE hash = ? LIMIT 1 ALLOW FILTERING`,
		hash).Consistency(gocql.One).Scan(&id, &url, &ts, &visited, &userId); err != nil {
		log.Warn(err)
	} else {
		if err := s.Query(`UPDATE url_shortener.urls SET visited = ? WHERE id = ?`,
			visited + 1, id).Exec(); err != nil {
			return urlStruct, err
		}
	}

	urlStruct = URL{id, url, hash, ts, visited, userId}
	return urlStruct, nil
}

func RunCassandra() (*gocql.Session, error) {
	dbAddress := os.Getenv("SHORTENER_DB")

	if len(dbAddress) == 0 {
		dbAddress = "localhost"
	}

	cluster := gocql.NewCluster(dbAddress)
	if dbAddress != "localhost" {
		cluster.Authenticator = gocql.PasswordAuthenticator{Username: os.Getenv("SHORTENER_DB_USER"), Password: os.Getenv("SHORTENER_DB_PASSWORD")}
	}

	cluster.Keyspace = "url_shortener"
	cluster.Consistency = gocql.Quorum

	session, err := cluster.CreateSession()
	s = session
	if err != nil {
		return nil, err
	}

	log.Println("Cassandra connected!")
	return s, nil
}
