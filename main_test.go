// nolint
package main

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"github.com/cyverse-de/configurate"
	"github.com/cyverse-de/messaging/v9"
	"github.com/cyverse-de/model"
	"github.com/spf13/viper"
	"github.com/streadway/amqp"
)

var (
	cfg *viper.Viper
)

func shouldrun() bool {
	if os.Getenv("RUN_INTEGRATION_TESTS") != "" {
		return true
	}
	return false
}

func rabbituri() string {
	return "amqp://guest:guest@rabbit:5672/"
}

func dburi() string {
	return "postgres://de:notprod@dedb:5432/de?sslmode=disable"
}

func initdb(t *testing.T) *sql.DB {
	db, err := sql.Open("postgres", dburi())
	if err != nil {
		t.Error(err)
	}
	err = db.Ping()
	if err != nil {
		t.Error(err)
	}
	return db
}

func inittests(t *testing.T) {
	var err error
	cfg, err = configurate.InitDefaults("../test/test_config.yaml", configurate.JobServicesDefaults)
	if err != nil {
		t.Error(err)
	}
}

func TestNew(t *testing.T) {
	n := New(cfg)

	if n == nil {
		t.Error("New returned nil")
	}

	if n.cfg != cfg {
		t.Error("Config objects did not match")
	}
}

func TestInsert(t *testing.T) {
	inittests(t)
	app := New(cfg)
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("an error '%s' was encountered when creating the mock database", err)
	}
	defer db.Close()
	app.db = db

	var lastInsertID int64
	result := sqlmock.NewResult(lastInsertID, 1)
	mock.ExpectExec("INSERT INTO job_status_updates").
		WithArgs("invID", "message", "state", "host", "ip", 0).
		WillReturnResult(result)

	_, err = app.insert("state", "invID", "message", "ip", "host", 0)
	if err != nil {
		t.Errorf("error was not expected updating job_status_updates: %s", err)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unfulfilled expectations inserting job_status_updates")
	}
}

type MockConsumer struct {
	exchange     string
	exchangeType string
	queue        string
	key          string
	handler      messaging.MessageHandler
}

type MockMessage struct {
	key string
	msg []byte
}

type MockMessenger struct {
	consumers         []MockConsumer
	publishedMessages []MockMessage
	publishTo         []string
	publishError      bool
}

func (m *MockMessenger) Close()  {}
func (m *MockMessenger) Listen() {}

func (m *MockMessenger) AddConsumer(exchange, exchangeType, queue, key string, handler messaging.MessageHandler, prefectCount int) {
	m.consumers = append(m.consumers, MockConsumer{
		exchange:     exchange,
		exchangeType: exchangeType,
		queue:        queue,
		key:          key,
		handler:      handler,
	})
}

func (m *MockMessenger) Publish(key string, msg []byte) error {
	if m.publishError {
		return errors.New("publish error")
	}
	m.publishedMessages = append(m.publishedMessages, MockMessage{key: key, msg: msg})
	return nil
}

func (m *MockMessenger) SetupPublishing(exchange string) error {

	m.publishTo = append(m.publishTo, exchange)
	return nil
}

func TestMsg(t *testing.T) {
	inittests(t)
	now := time.Now().Unix()
	nowstr := fmt.Sprintf("%d", now)
	testCases := []struct {
		State        string
		InvocationID string
		Message      string
		Sender       string
		SenderAddr   string
		SentOn       string
	}{
		{"State", "InvocationID", "Message", "127.0.0.1", "127.0.0.1", nowstr},
		{"", "InvocationID", "Message", "127.0.0.1", "127.0.0.1", nowstr},
		{"State", "", "Message", "127.0.0.1", "127.0.0.1", nowstr},
		{"State", "InvocationID", "", "127.0.0.1", "127.0.0.1", nowstr},
		{"State", "InvocationID", "Message", "", "0.0.0.0", nowstr},
		{"State", "InvocationID", "Message", "localhost", "localhost", nowstr},
		{"State", "InvocationID", "Message", "barf", "barf", nowstr},
		{"State", "InvocationID", "Message", "127.0.0.1", "127.0.0.1", ""},
	}

	for _, tc := range testCases {
		// Set up mock object for the database
		app := New(cfg)
		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatalf("an error '%s' was encountered when creating the mock database", err)
		}

		app.db = db

		// Set up mock object for the amqp stuff
		app.amqpClient = &MockMessenger{
			publishedMessages: make([]MockMessage, 0),
		}

		u := &messaging.UpdateMessage{
			State:   messaging.JobState(tc.State),
			Job:     model.New(cfg),
			Message: tc.Message,
			Sender:  tc.Sender,
			SentOn:  tc.SentOn,
		}
		u.Job.InvocationID = tc.InvocationID

		body, err := json.Marshal(u)
		if err != nil {
			t.Errorf("error marshalling delivery body: %s", err)
		}
		d := amqp.Delivery{
			RoutingKey: "not-ping",
			Body:       body,
		}

		var lastInsertID int64
		result := sqlmock.NewResult(lastInsertID, 1)
		if tc.Sender == "" {
			tc.Sender = "0.0.0.0"
		}

		n := now
		if tc.SentOn == "" {
			n = 0
		}

		if tc.Message == "" {
			tc.Message = "UNKNOWN"
		}

		if tc.Sender == "localhost" {
			tc.Sender = "127.0.0.1"
		}

		if tc.Sender == "barf" {
			tc.Sender = "0.0.0.0"
		}

		mock.ExpectExec("INSERT INTO job_status_updates.*").
			WithArgs(tc.InvocationID, tc.Message, tc.State, tc.Sender, tc.SenderAddr, n).
			WillReturnResult(result)

		// make the call
		app.msg(d)

		if tc.State == "" {
			return
		}

		// check the results
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Errorf("unfulfilled expectations inserting job_status_updates: %s", err)
		}
		db.Close()
	}
}
