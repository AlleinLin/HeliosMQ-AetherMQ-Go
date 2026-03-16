package common

import "errors"

var (
	ErrTopicNotFound      = errors.New("topic not found")
	ErrPartitionNotFound  = errors.New("partition not found")
	ErrBrokerUnavailable  = errors.New("broker unavailable")
	ErrNoLeader           = errors.New("no leader available")
	ErrRateLimitExceeded  = errors.New("rate limit exceeded")
	ErrInvalidOffset      = errors.New("invalid offset")
	ErrMessageTooLarge    = errors.New("message too large")
	ErrGroupNotFound      = errors.New("consumer group not found")
	ErrMemberNotFound     = errors.New("member not found")
	ErrTransactionAborted = errors.New("transaction aborted")
)
