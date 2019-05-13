package db

import (
	"github.com/gocql/gocql"
	log "github.com/sirupsen/logrus"
	"github.com/vladkampov/url-shortener/helpers"
	"time"
)

type URL struct{
	Id gocql.UUID
	Url string
	Hash string
	Ts time.Time
	Visited int32
}

var s *gocql.Session

func WriteURL(url string) (string, gocql.UUID) {
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

	if err := s.Query(`INSERT INTO url_shortener."urls" (id, url, hash, ts, visited) VALUES (?, ?, ?, ?, ?)`,
		id, url, hash, time.Now(), 0).Exec(); err != nil {
		log.Fatal(err)
	}

	return hash, id
}

func ReadURL(hash string) URL {
	var id gocql.UUID
	var url string
	var ts time.Time
	var visited int32

	if err := s.Query(`SELECT id, url, ts, visited FROM url_shortener.urls WHERE hash = ? LIMIT 1 ALLOW FILTERING`,
		hash).Consistency(gocql.One).Scan(&id, &url, &ts, &visited); err != nil {
		log.Fatal(err)
	}


	if err := s.Query(`UPDATE url_shortener.urls SET visited = ? WHERE id = ?`,
		visited + 1, id).Exec(); err != nil {
		log.Fatal(err)
	}

	return URL{id, url, hash, ts, visited}
}

func InitCassandra() *gocql.Session {
	cluster := gocql.NewCluster("localhost")
	cluster.Keyspace = "url_shortener"
	cluster.Consistency = gocql.Quorum

	session, err := cluster.CreateSession()
	s = session
	if err != nil {
		log.Fatal("Cassandra connection failed", err)
	}

	log.Println("Cassandra connected!")
	return s
}
