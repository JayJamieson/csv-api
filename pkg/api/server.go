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
	"github.com/JayJamieson/csv-api/pkg/sniff"
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
	config        Config
	router        *echo.Echo
	db            *db.DB
	registry      *sniff.FieldRegistry
	registryStore *sniff.RegistryStore
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

	// Migrations for the staged-import flow: mapping store (learning) and
	// import state (survives restart between /load and /commit).
	migrateCtx, cancelMigrate := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancelMigrate()
	if err := database.MigrateImportState(migrateCtx); err != nil {
		return nil, fmt.Errorf("failed to migrate import_state: %w", err)
	}
	mappingStore := &sniff.MappingStore{DB: database.Meta()}
	if err := mappingStore.Migrate(migrateCtx); err != nil {
		return nil, fmt.Errorf("failed to migrate csv_mappings: %w", err)
	}

	// Field-type registry: seeds field_type/field_synonym from the compiled-in
	// builtins on first run, then loads whatever's in the DB (builtins plus
	// anything added since through the field-types API) as the live registry
	// every proposal and commit goes through.
	registryStore := &sniff.RegistryStore{DB: database.Meta()}
	if err := registryStore.Migrate(migrateCtx); err != nil {
		return nil, fmt.Errorf("failed to migrate field_type registry: %w", err)
	}
	registry, err := registryStore.LoadRegistry(migrateCtx)
	if err != nil {
		return nil, fmt.Errorf("failed to load field_type registry: %w", err)
	}
	server.registry = registry
	server.registryStore = registryStore

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

	// Serve the built review UI (web/dist) at the root if present, so the SPA
	// and API share an origin. Registered routes (/api/:id, /load, /imports/*,
	// /swagger/*) are more specific than the static "/*" and keep priority. In
	// dev, run `cd web && npm run dev` (Vite proxies the API to this server)
	// instead of building; for production run `npm run build`.
	if _, err := os.Stat("web/dist/index.html"); err == nil {
		s.router.Static("/", "web/dist")
	} else {
		s.router.Logger.Info("web/dist not built; review UI not served (run: cd web && npm run build)")
	}
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
