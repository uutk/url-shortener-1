package db

import (
	"github.com/gocql/gocql"
	log "github.com/sirupsen/logrus"
	"github.com/vladkampov/url-shortener/helpers"
	"os"
	"time"
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

	if err := s.Query(`SELECT id, url, ts, visited, user_id FROM url_shortener.urls WHERE user_id = ? LIMIT 1 ALLOW FILTERING`,
		userId).Consistency(gocql.All).Scan(&urls); err != nil {
		return nil, err
	}

	return urls, nil
}

func WriteURL(url string, userId string) (string, error) {
	id := gocql.TimeUUID()
	hash := helpers.GetRandomString(6)

	// Recreate hash if there's an entity with same hash
	for true {
		if err := s.Query(`SELECT id, url, ts FROM url_shortener.urls WHERE hash = ? LIMIT 1 ALLOW FILTERING`,
			hash).Consistency(gocql.One).Scan(&id); err != nil {
			id = gocql.TimeUUID()
			break
		}
	}

	log.Println(id, url, hash, time.Now(), 0, userId)

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
