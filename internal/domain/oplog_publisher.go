package domain

// OplogPublisher defines the interface for publishing Oplog entries.
type OplogPublisher interface {
	PublishOplog(entry OplogEntry) error
	GetOplogs() (<-chan OplogEntry, error)
	Stop()
}

// InMemoryOplogPublisher is an in-memory implementation of the OplogPublisher.
type InMemoryOplogPublisher struct {
	channel chan OplogEntry
}

// NewInMemoryOplogPublisher creates a new instance of InMemoryOplogPublisher.
func NewInMemoryOplogPublisher() OplogPublisher {
	return &InMemoryOplogPublisher{
		channel: make(chan OplogEntry),
	}
}

// PublishOplog publishes the given Oplog entry by sending it to the channel.
func (p *InMemoryOplogPublisher) PublishOplog(entry OplogEntry) error {
	p.channel <- entry
	return nil
}

// GetOplogs retrieves a channel of Oplog entries from the in-memory publisher.
func (p *InMemoryOplogPublisher) GetOplogs() (<-chan OplogEntry, error) {
	return p.channel, nil
}

// Stop closes a channel of Oplog
func (p *InMemoryOplogPublisher) Stop() {
	close(p.channel)
}
