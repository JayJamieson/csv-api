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
	"github.com/labstack/echo/v4"
	"github.com/labstack/echo/v4/middleware"
	"github.com/labstack/gommon/log"
	echoSwagger "github.com/swaggo/echo-swagger"
)

type Config struct {
	Port        int
	DatabaseURL string
}

type Server struct {
	config Config
	router *echo.Echo
	db     *db.DB
}

func New(config Config) (*Server, error) {

	database, err := db.New(config.DatabaseURL)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize database: %w", err)
	}

	e := echo.New()

	server := &Server{
		config: config,
		router: e,
		db:     database,
	}

	if err != nil {
		return nil, fmt.Errorf("failed to load swagger: %w", err)
	}

	e.Use(middleware.Logger())
	e.Use(middleware.Recover())
	e.Use(middleware.CORS())

	e.Logger.SetLevel(log.INFO)

	RegisterHandlers(e, server)
	server.setupDefaultRoutes()
	return server, nil
}

func (s *Server) setupDefaultRoutes() {

	s.router.File("/doc.yml", "api-spec.yaml")
	s.router.GET("/swagger/*", echoSwagger.EchoWrapHandlerV3(func(c *echoSwagger.Config) {
		c.URLs = []string{"http://localhost:3000/doc.yml"}
	}))
}

func (s *Server) Start() error {
	go func() {
		addr := fmt.Sprintf(":%d", s.config.Port)
		if err := s.router.Start(addr); err != nil && err != http.ErrServerClosed {
			s.router.Logger.Fatalf("Failed to start server: %v", err)
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	s.router.Logger.Info("Shutting down")

	if err := s.router.Shutdown(ctx); err != nil {
		return fmt.Errorf("failed to shutdown server: %w", err)
	}

	if err := s.db.Close(); err != nil {
		return fmt.Errorf("failed to close database: %w", err)
	}

	return nil
}
