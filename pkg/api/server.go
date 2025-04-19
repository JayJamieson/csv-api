package api

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/JayJamieson/csv-api/pkg/db"
	"github.com/JayJamieson/csv-api/pkg/handlers"
	"github.com/labstack/echo/v4"
	"github.com/labstack/echo/v4/middleware"
)

type Config struct {
	Port        int
	DatabaseURL string
}

type Server struct {
	config   Config
	echo     *echo.Echo
	db       *db.DB
	handlers *handlers.Handler
}

func New(config Config) (*Server, error) {

	database, err := db.New(config.DatabaseURL)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize database: %w", err)
	}

	e := echo.New()

	e.Use(middleware.Logger())
	e.Use(middleware.Recover())
	e.Use(middleware.CORS())

	h := handlers.NewHandler(database)

	return &Server{
		config:   config,
		echo:     e,
		db:       database,
		handlers: h,
	}, nil
}

func (s *Server) setupRoutes() {

	s.echo.POST("/load", s.handlers.LoadCSV)

	s.echo.GET("/api/memory/:uuid", s.handlers.QueryMemoryCSV)
	s.echo.POST("/api/:uuid/persist", s.handlers.PersistCSV)

	s.echo.GET("/api/:uuid", s.handlers.QueryCSV)

	s.echo.File("/api-docs", "api-spec.yml")

	s.echo.GET("/", func(c echo.Context) error {
		return c.Redirect(http.StatusPermanentRedirect, "/api-docs")
	})
}

func (s *Server) Start() error {

	s.setupRoutes()

	go func() {
		addr := fmt.Sprintf(":%d", s.config.Port)
		if err := s.echo.Start(addr); err != nil && err != http.ErrServerClosed {
			s.echo.Logger.Fatalf("Failed to start server: %v", err)
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := s.echo.Shutdown(ctx); err != nil {
		return fmt.Errorf("failed to shutdown server: %w", err)
	}

	if err := s.db.Close(); err != nil {
		return fmt.Errorf("failed to close database: %w", err)
	}

	return nil
}
