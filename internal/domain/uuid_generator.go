package domain

import "github.com/brianvoe/gofakeit/v6"

type UUIDGenerator interface {
	UUID() string
}

// DefaultUUIDGenerator implements the UUIDGenerator interface using gofakeit.UUID.
type DefaultUUIDGenerator struct{}

func NewDefaultUUIDGenerator() UUIDGenerator {
	return &DefaultUUIDGenerator{}
}

// UUID generates a UUID using gofakeit.UUID.
func (g *DefaultUUIDGenerator) UUID() string {
	return gofakeit.UUID()
}
