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

	"github.com/cyverse-de/configurate"
	"github.com/cyverse-de/version"
	_ "github.com/lib/pq"
	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	"github.com/streadway/amqp"
	"gopkg.in/cyverse-de/messaging.v6"
)

var log = logrus.WithFields(logrus.Fields{"service": "job-status-recorder"})

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

func (r *JobStatusRecorder) msg(delivery amqp.Delivery) {
	redelivered := delivery.Redelivered

	log.Info("Message received")

	update := &messaging.UpdateMessage{}

	err := json.Unmarshal(delivery.Body, update)
	if err != nil {
		log.Error(err)
		err = delivery.Reject(!redelivered)
		if err != nil {
			log.Error(err)
		}
		return
	}

	if update.State == "" {
		log.Warn("State was unset, dropping update")
		return
	}
	log.Infof("State is %s\n", update.State)

	if update.Job.InvocationID == "" {
		log.Warn("InvocationID was unset, dropping update")
	}
	log.Infof("InvocationID is %s\n", update.Job.InvocationID)

	if update.Message == "" {
		log.Warn("Message set to empty string, setting to UNKNOWN")
		update.Message = "UNKNOWN"
	}
	log.Infof("Message is: %s", update.Message)

	var sentFromAddr string
	if update.Sender == "" {
		log.Warn("Unknown sender, setting from address to 0.0.0.0")
		update.Sender = "0.0.0.0"
	}

	parsedIP := net.ParseIP(update.Sender)
	if parsedIP != nil {
		sentFromAddr = update.Sender
	} else {
		ips, err := net.LookupIP(update.Sender)
		if err != nil {
			log.Warnf("Provided hostname %s did not resolve to an IP, setting from address to 0.0.0.0: %+v", update.Sender, err)
			sentFromAddr = "0.0.0.0"
		} else {
			if len(ips) > 0 {
				sentFromAddr = ips[0].String()
			}
		}
	}

	log.Infof("Sent from: %s", sentFromAddr)

	log.Infof("Sent On, unparsed: %s", update.SentOn)
	sentOn, err := strconv.ParseInt(update.SentOn, 10, 64)
	if err != nil {
		log.Errorf("Error parsing SentOn field, setting field to 0: %s", err)
		sentOn = 0
	}
	log.Infof("Sent On: %d", sentOn)

	result, err := r.insert(
		string(update.State),
		update.Job.InvocationID,
		update.Message,
		update.Sender,
		sentFromAddr,
		sentOn,
	)
	if err != nil {
		log.Error(err)
		err = delivery.Reject(!redelivered)
		if err != nil {
			log.Error(err)
		}
		return
	}

	rowCount, err := result.RowsAffected()
	if err != nil {
		log.Error(err)
		err = delivery.Reject(!redelivered)
		if err != nil {
			log.Error(err)
		}
		return
	}
	log.Infof("Inserted %d rows\n", rowCount)

	if err := delivery.Ack(false); err != nil {
		log.Error(err)
	}
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

	if *showVersion {
		version.AppVersion()
		os.Exit(0)
	}

	if *cfgPath == "" {
		log.Fatal("--config must be set.")
	}

	cfg, err = configurate.InitDefaults(*cfgPath, configurate.JobServicesDefaults)
	if err != nil {
		log.Fatal(err)
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

	log.Infof("AMQP broker setting is %s\n", *amqpURI)
	app.amqpClient, err = messaging.NewClient(*amqpURI, false)
	if err != nil {
		log.Fatal(err)
	}
	defer app.amqpClient.Close()

	if err = app.amqpClient.SetupPublishing(*amqpExchange); err != nil {
		log.Fatal(err)
	}

	log.Info("Connecting to the database...")
	app.db, err = sql.Open("postgres", *dbURI)
	if err != nil {
		log.Fatal(err)
	}
	err = app.db.Ping()
	if err != nil {
		log.Fatal(err)
	}
	log.Info("Connected to the database")

	go app.amqpClient.Listen()

	app.amqpClient.AddConsumer(
		*amqpExchange,
		*amqpType,
		"job_status_recorder",
		messaging.UpdatesKey,
		app.msg,
		100,
	)

	spinner := make(chan int)
	go func() {
		sock, err := net.Listen("tcp", "0.0.0.0:60000")
		if err != nil {
			log.Fatal(err)
		}
		err = http.Serve(sock, nil)
		if err != nil {
			log.Fatal(err)
		}
	}()
	<-spinner
}
