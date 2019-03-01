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
	"github.com/spf13/viper"
	"github.com/streadway/amqp"
	"gopkg.in/cyverse-de/messaging.v6"
	"gopkg.in/cyverse-de/model.v4"
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

func TestJobEvent(t *testing.T) {
	inittests(t)
	testCases := []struct {
		EventName   string
		ServiceName string
		Host        string
		AppId       string
		JobId       string
		JobState    string
		ExecutorId  string
		User        string
		Timestamp   int64
		Message     string
	}{
		{"event", "test-service", "host", "app-id", "job-id", "job-state", "executor-id", "user", 0, "message"},
		{"", "test-service", "host", "app-id", "job-id", "job-state", "executor-id", "user", 0, "message"},
		{"event", "", "host", "app-id", "job-id", "job-state", "executor-id", "user", 0, "message"},
		{"event", "test-service", "", "app-id", "job-id", "job-state", "executor-id", "user", 0, "message"},
		{"event", "test-service", "host", "", "job-id", "job-state", "executor-id", "user", 0, "message"},
		{"event", "test-service", "host", "app-id", "", "job-state", "executor-id", "user", 0, "message"},
		{"event", "test-service", "host", "app-id", "job-id", "", "executor-id", "user", 0, "message"},
		{"event", "test-service", "host", "app-id", "job-id", "job-state", "", "user", 0, "message"},
		{"event", "test-service", "host", "app-id", "job-id", "job-state", "executor-id", "", 0, "message"},
		{"event", "test-service", "host", "app-id", "job-id", "job-state", "executor-id", "user", 1, "message"},
		{"event", "test-service", "host", "app-id", "job-id", "job-state", "executor-id", "user", 0, ""},
	}
	for _, tc := range testCases {
		t.Run(fmt.Sprintf("%s-%s-%s-%s-%s-%s-%s-%s-%d-%s", tc.EventName, tc.ServiceName, tc.Host, tc.AppId, tc.JobId, tc.JobState, tc.ExecutorId, tc.User, tc.Timestamp, tc.Message), func(t *testing.T) {
			job := model.New(cfg)
			job.InvocationID = tc.JobId
			job.AppID = tc.AppId
			job.CondorID = tc.ExecutorId
			job.Submitter = tc.User
			update := &messaging.UpdateMessage{
				State:   messaging.JobState(tc.JobState),
				Job:     job,
				Message: tc.Message,
				Sender:  tc.Host,
				SentOn:  string(tc.Timestamp),
			}
			e := jobEvent(tc.EventName, tc.ServiceName, tc.Host, tc.Timestamp, update)
			if e.EventName != tc.EventName {
				t.Errorf("event name was %s instead of %s", e.EventName, tc.EventName)
			}
			if e.ServiceName != tc.ServiceName {
				t.Errorf("service name was %s instead of %s", e.ServiceName, tc.ServiceName)
			}
			if e.Host != tc.Host {
				t.Errorf("host was %s instead of %s", e.Host, tc.Host)
			}
			if e.AppId != tc.AppId {
				t.Errorf("app id was %s instead of %s", e.AppId, tc.AppId)
			}
			if e.JobId != tc.JobId {
				t.Errorf("job id was %s instead of %s", e.JobId, tc.JobId)
			}
			if e.JobState != tc.JobState {
				t.Errorf("state was %s instead of %s", e.JobState, tc.JobState)
			}
			if e.ExecutorId != tc.ExecutorId {
				t.Errorf("executor id was %s instead of %s", e.ExecutorId, tc.ExecutorId)
			}
			if e.User != tc.User {
				t.Errorf("user was %s instead of %s", e.User, tc.User)
			}
			if e.Timestamp != tc.Timestamp {
				t.Errorf("timestamp was %d instead of %d", e.Timestamp, tc.Timestamp)
			}
			if e.Message != tc.Message {
				t.Errorf("message was %s instead of %s", e.Message, tc.Message)
			}
		})
	}
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
