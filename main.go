package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/gosom/google-maps-scraper/runner"
	"github.com/gosom/google-maps-scraper/runner/databaserunner"
	"github.com/gosom/google-maps-scraper/runner/filerunner"
	"github.com/gosom/google-maps-scraper/runner/installplaywright"
	"github.com/gosom/google-maps-scraper/runner/lambdaaws"
	"github.com/gosom/google-maps-scraper/runner/webrunner"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var kafkaConfig runner.KafkaConfig
var databases runner.Databases
var mongoClient *mongo.Database

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	runner.Banner()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	go func() {
		<-sigChan
		log.Println("Received signal, shutting down...")
		cancel()
	}()

	loggerConfig := zap.NewProductionConfig()
	loggerLevel, _ := zapcore.ParseLevel("info")
	loggerConfig.Level = zap.NewAtomicLevelAt(loggerLevel)

	logger, _ := loggerConfig.Build(
		zap.AddCaller(),
		zap.AddCallerSkip(1),
		zap.AddStacktrace(zapcore.ErrorLevel),
	)
	defer logger.Sync()

	cfg := runner.ParseConfig()

	sqlURI := os.Getenv("SQL_URI")
	if sqlURI == "" {
		log.Panic("Missing required environment variable: SQL_URI")
	}
	cfg.Databases.Discovery.URI = sqlURI
	cfg.MongoClient = mongoClient

	runnerInstance, err := runnerFactory(cfg)
	if err != nil {
		cancel()
		os.Stderr.WriteString(err.Error() + "\n")
		runner.Telemetry().Close()
		os.Exit(1)
	}

	if err := runnerInstance.Run(ctx); err != nil {
		os.Stderr.WriteString(err.Error() + "\n")
		_ = runnerInstance.Close(ctx)
		runner.Telemetry().Close()
		cancel()
		os.Exit(1)
	}

	_ = runnerInstance.Close(ctx)
	runner.Telemetry().Close()
	cancel()
	os.Exit(0)
}

func runnerFactory(cfg *runner.Config) (runner.Runner, error) {
	switch cfg.RunMode {
	case runner.RunModeFile:
		return filerunner.New(cfg)
	case runner.RunModeDatabase, runner.RunModeDatabaseProduce:
		return databaserunner.New(cfg)
	case runner.RunModeInstallPlaywright:
		return installplaywright.New(cfg)
	case runner.RunModeWeb:
		return webrunner.New(cfg)
	case runner.RunModeAwsLambda:
		return lambdaaws.New(cfg)
	case runner.RunModeAwsLambdaInvoker:
		return lambdaaws.NewInvoker(cfg)
	default:
		return nil, fmt.Errorf("%w: %d", runner.ErrInvalidRunMode, cfg.RunMode)
	}
}

func init() {
	mongoURI := os.Getenv("MONGODB_URI")
	if mongoURI == "" {
		log.Panic("Missing required environment variable: MONGODB_URI")
	}

	db, err := NewMongoClient(mongoURI, "staging-auth")
	if err != nil {
		log.Panic("Failed to connect to MongoDB:", err)
	}
	mongoClient = db
}

func NewMongoClient(connectionURI, databaseName string) (*mongo.Database, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	client, err := mongo.Connect(ctx, options.Client().ApplyURI(connectionURI))
	if err != nil {
		log.Println(err)
		return nil, err
	}

	if err := client.Ping(ctx, nil); err != nil {
		log.Println("InitMongoClient-err", err)
		return nil, err
	}

	log.Println("Connected to MongoDB!")
	return client.Database(databaseName), nil
}
