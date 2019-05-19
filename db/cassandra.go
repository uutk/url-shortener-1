package db

import (
	"github.com/gocql/gocql"
	"github.com/mitchellh/mapstructure"
	log "github.com/sirupsen/logrus"
	"github.com/vladkampov/url-shortener/helpers"
	"os"
	"time"
)

type URL struct {
	Id gocql.UUID
	Url string
	Hash string
	Ts time.Time
	Visited int32
	UserId gocql.UUID
}

type User struct {
	Id gocql.UUID
	ForeignId string
	Tokens int32
	CustomDomain string
}

var s *gocql.Session

func CreateUser(userId string) (*gocql.UUID, error) {
	log.Printf("No user with foreign id %s. Creating new one...", userId)

	systemUserId := gocql.TimeUUID()
	if err := s.Query(`INSERT INTO url_shortener.users (id, foreign_id, tokens) VALUES (?, ?, ?)`,
		systemUserId, userId, 100).Exec(); err != nil {
		return nil, err
	}

	log.Printf("User created with id %s.", systemUserId)
	return &systemUserId, nil
}

func GetUserByForeignKey(userId string) (*User, error) {
	var systemUserId gocql.UUID
	var customDomain string
	var tokens int32


	log.Printf("Getting user by foreign id %s.", userId)
	err := s.Query(`SELECT id, custom_domain, tokens FROM url_shortener.users WHERE foreign_id = ? LIMIT 1 ALLOW FILTERING`,
		userId).Consistency(gocql.One).Scan(&systemUserId, &customDomain, &tokens)

	if  err != nil {
		return nil, err
	}

	log.Printf("Got user by foreign id %s. His foreign id is %s", userId, systemUserId)
	return &User{Id: systemUserId, CustomDomain: customDomain, Tokens: tokens, ForeignId: userId}, nil
}

func AddCustomDomainToUser(userId string, customDomain string) (*User, error) {
	log.Printf("Adding custom domain %s to the user by foreign id %s.", customDomain, userId)
	user, err := GetUserByForeignKey(userId)
	if err != nil {
		return nil, err
	}

	err = s.Query(`UPDATE url_shortener.users SET custom_domain = ? WHERE id = ?`,
		customDomain, user.Id).Exec()
	if  err != nil {
		return nil, err
	}

	user, err = GetUserByForeignKey(userId)
	if err != nil {
		return nil, err
	}

	log.Printf("User successfully updated: id: %s, foreign_id: %s, custom domain: %s, tokens: %d", user.Id.String(), user.ForeignId, user.CustomDomain, user.Tokens)
	return user, nil
}

func AddUserTokens(userId string, amount int32) (*User, error) {
	log.Printf("Increating amount of user token by foreign id: %s", userId)
	user, err := GetUserByForeignKey(userId)
	if err != nil {
		return nil, err
	}

	err = s.Query(`UPDATE url_shortener.users SET tokens = ? WHERE id = ?`,
		user.Tokens + amount, user.Id).Exec()
	if  err != nil {
		return nil, err
	}

	user, err = GetUserByForeignKey(userId)
	if err != nil {
		return nil, err
	}

	log.Printf("User successfully updated: id: %s, foreign_id: %s, custom domain: %s, tokens: %d", user.Id.String(), user.ForeignId, user.CustomDomain, user.Tokens)
	return user, nil
}

func DecUserTokens(userId string) (*User, error) {
	log.Printf("Decreasing amount of user token by foreign id: %s", userId)
	user, err := GetUserByForeignKey(userId)
	if err != nil {
		return nil, err
	}

	err = s.Query(`UPDATE url_shortener.users SET tokens = ? WHERE id = ?`,
		user.Tokens - 1, user.Id).Exec()
	if  err != nil {
		return nil, err
	}

	user, err = GetUserByForeignKey(userId)
	if err != nil {
		return nil, err
	}

	log.Printf("User successfully updated: id: %s, foreign_id: %s, custom domain: %s, tokens: %d", user.Id.String(), user.ForeignId, user.CustomDomain, user.Tokens)
	return user, nil
}

func ReadURLsByUserId(userId string) ([]URL, error) {
	var urls []URL
	user, err := GetUserByForeignKey(userId)
	if err != nil {
		return urls, err
	}

	iter := s.Query(`SELECT id, url, hash, ts, visited FROM url_shortener.urls WHERE user_id = ? ALLOW FILTERING`,
		user.Id).Consistency(gocql.All).Iter()

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

	var systemUserId *gocql.UUID
	user, err := GetUserByForeignKey(userId)
	if  err != nil {
		systemUserId, err = CreateUser(userId)

		if err != nil {
			return "", nil
		}
	} else {
		systemUserId = &user.Id
	}

	// return url if it was already shortened for current user
	var hashFromDB string
	err = s.Query(`SELECT hash FROM url_shortener.urls WHERE url = ? AND user_id = ? LIMIT 1 ALLOW FILTERING`,
		url, systemUserId).Consistency(gocql.One).Scan(&hashFromDB)

	if  err == nil {
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
	if err := s.Query(`INSERT INTO url_shortener.urls (id, url, hash, ts, visited, user_id) VALUES (?, ?, ?, ?, ?, ?)`,
		id, url, hash, time.Now(), 0, systemUserId).Exec(); err != nil {
		return "", err
	}

	if len(user.CustomDomain) != 0 {
		user, err = DecUserTokens(userId)
		if err != nil {
			log.Warnf("Error decreasing user tokens amount: %s", err)
			return hash, nil
		}
	}

	return hash, nil
}

func ReadURL(hash string) (URL, error) {
	var id gocql.UUID
	var url string
	var ts time.Time
	var visited int32
	var userId gocql.UUID
	var urlStruct URL

	if err := s.Query(`SELECT id, url, ts, visited, user_id FROM url_shortener.urls WHERE hash = ? LIMIT 1 ALLOW FILTERING`,
		hash).Consistency(gocql.One).Scan(&id, &url, &ts, &visited, &userId); err != nil {
		return urlStruct, err
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
	cluster.ProtoVersion = 4
	cluster.CQLVersion = "3.4.4"

	session, err := cluster.CreateSession()
	if err != nil {
		return nil, err
	}
	s = session

	log.Println("Cassandra connected!")
	return s, nil
}
