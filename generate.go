package api

// This file contains the go:generate directives for code generation.
// Run `go generate ./...` from the project root to regenerate code.

//go:generate go tool oapi-codegen --config=codegen.yaml api-spec.yaml
