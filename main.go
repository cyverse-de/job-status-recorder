// job-status-recorder
//
// This service listens for job updates sent via AMQP on the "jobs" exchange
// with a key of "jobs.updates". Each update is recorded in the DE database's
// job_status_updates table.
//
package main

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	_ "expvar"
	"flag"
	"net"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/cyverse-de/configurate"
	"github.com/cyverse-de/messaging/v9"
	"github.com/cyverse-de/version"
	_ "github.com/lib/pq"
	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	"github.com/streadway/amqp"

	"github.com/uptrace/opentelemetry-go-extra/otelsql"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/jaeger"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	tracesdk "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.7.0"
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

func (r *JobStatusRecorder) insert(ctx context.Context, state, invID, msg, host, ip string, sentOn int64) (sql.Result, error) {
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
	return r.db.ExecContext(ctx, insertStr, invID, msg, state, ip, host, sentOn)
}

func (r *JobStatusRecorder) msg(ctx context.Context, delivery amqp.Delivery) {
	start := time.Now()

	redelivered := delivery.Redelivered
	l := log.WithFields(logrus.Fields{"redelivered": redelivered})

	update := &messaging.UpdateMessage{}

	err := json.Unmarshal(delivery.Body, update)
	if err != nil {
		l.Error(err)
		err = delivery.Reject(!redelivered)
		if err != nil {
			l.Error(err)
		}
		return
	}

	if update.State == "" {
		l.Warn("State was unset, dropping update")
		return
	}
	l = l.WithFields(logrus.Fields{"state": update.State})

	if update.Job.InvocationID == "" {
		l.Warn("InvocationID was unset, dropping update")
	}
	l = l.WithFields(logrus.Fields{"invocationID": update.Job.InvocationID})

	if update.Message == "" {
		l.Warn("Message set to empty string, setting to UNKNOWN")
		update.Message = "UNKNOWN"
	}
	l = l.WithFields(logrus.Fields{"updateMessage": update.Message})

	var sentFromAddr string
	if update.Sender == "" {
		l.Warn("Unknown sender, setting from address to 0.0.0.0")
		update.Sender = "0.0.0.0"
	}
	l = l.WithFields(logrus.Fields{"sender": update.Sender})

	parsedIP := net.ParseIP(update.Sender)
	if parsedIP != nil {
		sentFromAddr = update.Sender
	} else {
		ips, err := net.LookupIP(update.Sender)
		if err != nil {
			l.Warnf("Provided hostname %s did not resolve to an IP, setting from address to 0.0.0.0: %+v", update.Sender, err)
			sentFromAddr = "0.0.0.0"
		} else {
			if len(ips) > 0 {
				sentFromAddr = ips[0].String()
			}
		}
	}
	l = l.WithFields(logrus.Fields{"sentFromAddr": sentFromAddr, "sentOn": update.SentOn})

	sentOn, err := strconv.ParseInt(update.SentOn, 10, 64)
	if err != nil {
		l.Errorf("Error parsing SentOn field, setting field to 0: %s", err)
		sentOn = 0
	}

	result, err := r.insert(
		ctx,
		string(update.State),
		update.Job.InvocationID,
		update.Message,
		update.Sender,
		sentFromAddr,
		sentOn,
	)
	if err != nil {
		l.Error(err)
		err = delivery.Reject(!redelivered)
		if err != nil {
			l.Error(err)
		}
		return
	}

	rowCount, err := result.RowsAffected()
	if err != nil {
		l.Error(err)
		err = delivery.Reject(!redelivered)
		if err != nil {
			l.Error(err)
		}
		return
	}
	elapsed := time.Since(start)
	l.Infof("Processed message and inserted %d rows in %s", rowCount, elapsed)

	if err := delivery.Ack(false); err != nil {
		l.Error(err)
	}
}

func jaegerTracerProvider(url string) (*tracesdk.TracerProvider, error) {
	// Create the Jaeger exporter
	exp, err := jaeger.New(jaeger.WithCollectorEndpoint(jaeger.WithEndpoint(url)))
	if err != nil {
		return nil, err
	}

	tp := tracesdk.NewTracerProvider(
		tracesdk.WithBatcher(exp),
		tracesdk.WithResource(resource.NewWithAttributes(
			semconv.SchemaURL,
			semconv.ServiceNameKey.String("job-status-recorder"),
		)),
	)

	return tp, nil
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

		tracerProvider *tracesdk.TracerProvider
	)

	otelTracesExporter := os.Getenv("OTEL_TRACES_EXPORTER")
	if otelTracesExporter == "jaeger" {
		jaegerEndpoint := os.Getenv("OTEL_EXPORTER_JAEGER_ENDPOINT")
		if jaegerEndpoint == "" {
			log.Warn("Jaeger set as OpenTelemetry trace exporter, but no Jaeger endpoint configured.")
		} else {
			tp, err := jaegerTracerProvider(jaegerEndpoint)
			if err != nil {
				log.Fatal(err)
			}
			tracerProvider = tp
			otel.SetTracerProvider(tp)
			otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{}))
		}
	}

	if tracerProvider != nil {
		tracerCtx, cancel := context.WithCancel(context.Background())
		defer cancel()

		defer func(tracerContext context.Context) {
			ctx, cancel := context.WithTimeout(tracerContext, time.Second*5)
			defer cancel()
			if err := tracerProvider.Shutdown(ctx); err != nil {
				log.Fatal(err)
			}
		}(tracerCtx)
	}

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
	app.db, err = otelsql.Open("postgres", *dbURI,
		otelsql.WithAttributes(semconv.DBSystemPostgreSQL),
	)
	if err != nil && err != driver.ErrSkip {
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
