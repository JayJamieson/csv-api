package main

import (
	"flag"
	"fmt"
	"log"
	"os"

	"github.com/JayJamieson/csv-api/pkg/api"
)

func main() {

	port := flag.Int("port", 8001, "Server port")
	dbURL := flag.String("db-url", "file:data.db", "Turso database URL")
	flag.Parse()

	if envPort := os.Getenv("PORT"); envPort != "" {
		if p, err := fmt.Sscanf(envPort, "%d", port); err != nil || p != 1 {
			log.Printf("Invalid PORT environment variable: %s, using default: %d", envPort, *port)
		}
	}

	if envDBURL := os.Getenv("DATABASE_URL"); envDBURL != "" {
		*dbURL = envDBURL
	}

	config := api.Config{
		Port:        *port,
		DatabaseURL: *dbURL,
	}

	server, err := api.New(config)

	if err != nil {
		log.Fatalf("Failed to create server: %v", err)
	}

	if err := server.Start(); err != nil {
		log.Fatalf("Server error: %v", err)
	}
}
