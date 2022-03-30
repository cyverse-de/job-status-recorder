module github.com/cyverse-de/job-status-recorder

go 1.16

require (
	github.com/DATA-DOG/go-sqlmock v1.1.4-0.20160722192640-05f39e9110c0
	github.com/cyverse-de/configurate v0.0.0-20190318152107-8f767cb828d9
	github.com/cyverse-de/messaging/v9 v9.1.3
	github.com/cyverse-de/model/v6 v6.0.1
	github.com/cyverse-de/version v0.0.0-20160721234331-5119d6500655
	github.com/lib/pq v1.10.4
	github.com/magiconair/properties v1.8.6 // indirect
	github.com/sirupsen/logrus v1.8.1
	github.com/spf13/afero v1.8.2 // indirect
	github.com/spf13/viper v1.10.1
	github.com/streadway/amqp v1.0.1-0.20200716223359-e6b33f460591
	github.com/uptrace/opentelemetry-go-extra/otelsql v0.1.10
	go.opentelemetry.io/otel v1.6.1
	go.opentelemetry.io/otel/exporters/jaeger v1.6.1
	go.opentelemetry.io/otel/sdk v1.6.1
	golang.org/x/sys v0.0.0-20220330033206-e17cdc41300f // indirect
	gopkg.in/check.v1 v1.0.0-20201130134442-10cb98267c6c // indirect
	gopkg.in/ini.v1 v1.66.4 // indirect
)
