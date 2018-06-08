// job-status-recorder
//
// This service listens for job updates sent via AMQP on the "jobs" exchange
// with a key of "jobs.updates". Each update is recorded in the DE database's
// job_status_updates table.
//
package main

import (
	"database/sql"
	"encoding/json"
	_ "expvar"
	"flag"
	"net"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/cyverse-de/configurate"
	"github.com/cyverse-de/go-events/jobevents"
	"github.com/cyverse-de/go-events/ping"
	"github.com/cyverse-de/logcabin"
	"github.com/cyverse-de/version"
	_ "github.com/lib/pq"
	"github.com/spf13/viper"
	"github.com/streadway/amqp"
	"gopkg.in/cyverse-de/messaging.v4"
)

// Messenger defines an interface for handling AMQP operations. This is the
// subset of functionality needed by job-status-recorder.
type Messenger interface {
	AddConsumer(string, string, string, string, messaging.MessageHandler, int)
	Close()
	Listen()
	Publish(string, []byte) error
	SetupPublishing(string) error
}

// JobStatusRecorder contains the application state for job-status-recorder
type JobStatusRecorder struct {
	cfg        *viper.Viper
	amqpClient Messenger
	db         *sql.DB
}

const pingKey = "events.job-status-recorder.ping"
const pongKey = "events.job-status-recorder.pong"
const storeKey = "events.job-status-recorder.record-status"

// New returns a *JobStatusRecorder
func New(cfg *viper.Viper) *JobStatusRecorder {
	return &JobStatusRecorder{
		cfg: cfg,
	}
}

func (r *JobStatusRecorder) insert(state, invID, msg, host, ip string, sentOn int64) (sql.Result, error) {
	insertStr := `
		INSERT INTO job_status_updates (
			external_id,
			message,
			status,
			sent_from,
			sent_from_hostname,
			sent_on
		) VALUES (
			$1,
			$2,
			$3,
			$4,
			$5,
			$6
		) RETURNING id`
	return r.db.Exec(insertStr, invID, msg, state, ip, host, sentOn)
}

func jobEvent(event, service, host string, now int64, update *messaging.UpdateMessage) *jobevents.JobEvent {
	return &jobevents.JobEvent{
		EventName:   event,
		ServiceName: service,
		Host:        host,
		AppId:       update.Job.AppID,
		JobId:       update.Job.InvocationID,
		JobState:    string(update.State),
		ExecutorId:  update.Job.CondorID,
		User:        update.Job.Submitter,
		Timestamp:   now,
		Message:     update.Message,
	}
}

func hostname() string {
	h, err := os.Hostname()
	if err != nil {
		return ""
	}
	return h
}

func (r *JobStatusRecorder) sendJobEvent(e *jobevents.JobEvent) error {
	out, err := json.Marshal(e)
	if err != nil {
		return err
	}

	return r.amqpClient.Publish(storeKey, out)
}

func (r *JobStatusRecorder) emitEvent(name, service string, update *messaging.UpdateMessage) {
	je := jobEvent(
		name,    //"record-job-status",
		service, //"job-status-recorder",
		hostname(),
		time.Now().Unix(),
		update,
	)

	if err := r.sendJobEvent(je); err != nil {
		logcabin.Error.Print(err)
	}
}

func (r *JobStatusRecorder) emitEventMessage(name, service, message string, update *messaging.UpdateMessage) {
	um := &messaging.UpdateMessage{
		Job:     update.Job,
		State:   update.State,
		Message: message,
	}
	r.emitEvent(name, service, um)
}

func (r *JobStatusRecorder) pingHandler(delivery amqp.Delivery) {
	logcabin.Info.Println("Received ping")

	out, err := json.Marshal(&ping.Pong{})
	if err != nil {
		logcabin.Error.Print(err)
	}

	logcabin.Info.Println("Sent pong")

	if err = r.amqpClient.Publish(pongKey, out); err != nil {
		logcabin.Error.Print(err)
	}
}

func (r *JobStatusRecorder) eventsHandler(delivery amqp.Delivery) {
	if err := delivery.Ack(false); err != nil {
		logcabin.Error.Print(err)
	}

	logcabin.Info.Println("event message received")

	if delivery.RoutingKey == pingKey {
		r.pingHandler(delivery)
	}
}

func (r *JobStatusRecorder) msg(delivery amqp.Delivery) {
	if err := delivery.Ack(false); err != nil {
		logcabin.Error.Print(err)
	}

	if delivery.RoutingKey == pingKey {
		r.pingHandler(delivery)
		return
	}

	logcabin.Info.Println("Message received")

	update := &messaging.UpdateMessage{}

	err := json.Unmarshal(delivery.Body, update)
	if err != nil {
		logcabin.Error.Print(err)
		return
	}

	if update.State == "" {
		logcabin.Warning.Println("State was unset, dropping update")
		return
	}
	logcabin.Info.Printf("State is %s\n", update.State)

	if update.Job.InvocationID == "" {
		logcabin.Warning.Println("InvocationID was unset, dropping update")
	}
	logcabin.Info.Printf("InvocationID is %s\n", update.Job.InvocationID)

	if update.Message == "" {
		logcabin.Warning.Println("Message set to empty string, setting to UNKNOWN")
		update.Message = "UNKNOWN"
	}
	logcabin.Info.Printf("Message is: %s", update.Message)

	var sentFromAddr string
	if update.Sender == "" {
		logcabin.Warning.Println("Unknown sender, setting from address to 0.0.0.0")
		update.Sender = "0.0.0.0"
	}

	parsedIP := net.ParseIP(update.Sender)
	if parsedIP != nil {
		sentFromAddr = update.Sender
	} else {
		ips, err := net.LookupIP(update.Sender)
		if err != nil {
			logcabin.Warning.Printf("Provided hostname %s did not resolve to an IP, setting from address to 0.0.0.0: %+v", update.Sender, err)
			sentFromAddr = "0.0.0.0"
		} else {
			if len(ips) > 0 {
				sentFromAddr = ips[0].String()
			}
		}
	}

	logcabin.Info.Printf("Sent from: %s", sentFromAddr)

	logcabin.Info.Printf("Sent On, unparsed: %s", update.SentOn)
	sentOn, err := strconv.ParseInt(update.SentOn, 10, 64)
	if err != nil {
		logcabin.Error.Printf("Error parsing SentOn field, setting field to 0: %s", err)
		sentOn = 0
	}
	logcabin.Info.Printf("Sent On: %d", sentOn)

	result, err := r.insert(
		string(update.State),
		update.Job.InvocationID,
		update.Message,
		update.Sender,
		sentFromAddr,
		sentOn,
	)
	if err != nil {
		r.emitEventMessage("record-job-status-error", "job-status-recorder", err.Error(), update)
		logcabin.Error.Print(err)
		return
	}

	r.emitEvent("record-job-status", "job-status-recorder", update)

	rowCount, err := result.RowsAffected()
	if err != nil {
		logcabin.Error.Print(err)
		return
	}

	lastInsertID, err := result.LastInsertId()
	if err != nil {
		logcabin.Error.Print(err)
		return
	}
	logcabin.Info.Printf("last insert ID: %d\n", lastInsertID)
	logcabin.Info.Printf("Inserted %d rows\n", rowCount)
}

func main() {
	var (
		err          error
		app          *JobStatusRecorder
		cfg          *viper.Viper
		showVersion  = flag.Bool("version", false, "Print the version information")
		cfgPath      = flag.String("config", "/etc/iplant/de/jobservices.yml", "The path to the config file")
		dbURI        = flag.String("db", "", "The URI used to connect to the database")
		amqpURI      = flag.String("amqp", "", "The URI used to connect to the amqp broker")
		amqpExchange = flag.String("exchange", "de", "The AMQP exchange to connect to")
		amqpType     = flag.String("exchangetype", "topic", "The type of the AMQP exchange")
	)

	flag.Parse()

	logcabin.Init("job-status-recorder", "job-status-recorder")

	if *showVersion {
		version.AppVersion()
		os.Exit(0)
	}

	if *cfgPath == "" {
		logcabin.Error.Fatal("--config must be set.")
	}

	cfg, err = configurate.InitDefaults(*cfgPath, configurate.JobServicesDefaults)
	if err != nil {
		logcabin.Error.Fatal(err)
	}

	if *dbURI == "" {
		*dbURI = cfg.GetString("db.uri")
	} else {
		cfg.Set("db.uri", *dbURI)
	}

	if *amqpURI == "" {
		*amqpURI = cfg.GetString("amqp.uri")
	} else {
		cfg.Set("amqp.uri", *amqpURI)
	}

	if *amqpExchange == "" {
		*amqpExchange = cfg.GetString("amqp.exchange.name")
	} else {
		cfg.Set("amqp.exchange.name", *amqpExchange)
	}

	if *amqpType == "" {
		*amqpType = cfg.GetString("amqp.exchange.type")
	} else {
		cfg.Set("amqp.exchange.type", *amqpType)
	}

	app = New(cfg)

	logcabin.Info.Printf("AMQP broker setting is %s\n", *amqpURI)
	app.amqpClient, err = messaging.NewClient(*amqpURI, false)
	if err != nil {
		logcabin.Error.Fatal(err)
	}
	defer app.amqpClient.Close()

	if err = app.amqpClient.SetupPublishing(*amqpExchange); err != nil {
		logcabin.Error.Fatal(err)
	}

	logcabin.Info.Println("Connecting to the database...")
	app.db, err = sql.Open("postgres", *dbURI)
	if err != nil {
		logcabin.Error.Fatal(err)
	}
	err = app.db.Ping()
	if err != nil {
		logcabin.Error.Fatal(err)
	}
	logcabin.Info.Println("Connected to the database")

	go app.amqpClient.Listen()

	app.amqpClient.AddConsumer(
		*amqpExchange,
		*amqpType,
		"job_status_recorder",
		messaging.UpdatesKey,
		app.msg,
		0,
	)

	app.amqpClient.AddConsumer(
		*amqpExchange,
		*amqpType,
		"job_status_recorder_events",
		"events.job-status-recorder.*",
		app.eventsHandler,
		0,
	)

	spinner := make(chan int)
	go func() {
		sock, err := net.Listen("tcp", "0.0.0.0:60000")
		if err != nil {
			logcabin.Error.Fatal(err)
		}
		http.Serve(sock, nil)
	}()
	<-spinner
}
