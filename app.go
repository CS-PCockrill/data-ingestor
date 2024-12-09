package main

import (
	"data-ingestor/config"
	"database/sql"
	"fmt"
	"go.uber.org/zap"
)

type App struct {
	Config *config.Config
	Logger    *zap.Logger
	DB *sql.DB
}

// NewApp initializes the App with dependencies
func NewApp() (*App, error) {
	cfg := config.GetConfig()

	logger, err := zap.NewProduction()
	if err != nil {
		return nil, fmt.Errorf("fatal error initializing logger: %w", err)
	}

	dsn := fmt.Sprintf("postgres://%s:%s@%s:%s/%s", cfg.DB.DBUser, cfg.DB.DBPassword, cfg.DB.DBHostname, cfg.DB.DBPort, cfg.DB.DBName)
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return nil, fmt.Errorf("fatal error connecting to database: %w", err)
	}

	// Test the connection
	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("failed to ping after connecting to database: %w", err)
	}

	db.SetMaxOpenConns(cfg.Runtime.WorkerCount)
	return &App{Config: cfg, Logger: logger, DB: db}, nil
}

func (app *App) Close() {
	app.Logger.Sync()
	app.DB.Close()
}


