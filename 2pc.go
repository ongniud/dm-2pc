package twopc

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log"

	"github.com/go-redis/redis/v8"
)

// TransferRequest defines the structure of a transfer request.
type TransferRequest struct {
	Xid    string `json:"xid"`
	From   string `json:"from"`
	To     string `json:"to"`
	Amount int    `json:"amount"`
}

// Participant is an interface for transaction participants.
type Participant interface {
	Prepare(ctx context.Context, req TransferRequest) (*Snapshot, error)
	Commit(ctx context.Context, req TransferRequest) error
	Rollback(ctx context.Context, req TransferRequest) error
	Verify(ctx context.Context, req TransferRequest, snapshot *Snapshot) (bool, error)
}

// Snapshot represents the state of accounts before a transaction.
type Snapshot struct {
	Data map[string]interface{}
}

// RedisParticipant implements the Participant interface for Redis.
type RedisParticipant struct {
	rdb *redis.Client
}

// NewRedisParticipant creates a new RedisParticipant instance.
func NewRedisParticipant(rdb *redis.Client) *RedisParticipant {
	return &RedisParticipant{
		rdb: rdb,
	}
}

// Prepare checks the balance in Redis and creates a snapshot.
func (r *RedisParticipant) Prepare(ctx context.Context, req TransferRequest) (*Snapshot, error) {
	fromBalance, err := r.rdb.Get(ctx, req.From).Int()
	if err != nil {
		return nil, fmt.Errorf("failed to get balance: %w", err)
	}
	if fromBalance < req.Amount {
		return nil, errors.New("insufficient balance")
	}
	toBalance, err := r.rdb.Get(ctx, req.To).Int()
	if err != nil {
		return nil, fmt.Errorf("failed to get balance: %w", err)
	}
	return &Snapshot{
		Data: map[string]interface{}{
			"From":        req.From,
			"FromBalance": fromBalance,
			"To":          req.To,
			"ToBalance":   toBalance,
		},
	}, nil
}

// Commit performs atomic operations in Redis.
func (r *RedisParticipant) Commit(ctx context.Context, req TransferRequest) error {
	_, err := r.rdb.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
		pipe.DecrBy(ctx, req.From, int64(req.Amount))
		pipe.IncrBy(ctx, req.To, int64(req.Amount))
		return nil
	})
	return err
}

// Rollback performs reverse atomic operations in Redis.
func (r *RedisParticipant) Rollback(ctx context.Context, req TransferRequest) error {
	_, err := r.rdb.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
		pipe.IncrBy(ctx, req.From, int64(req.Amount))
		pipe.DecrBy(ctx, req.To, int64(req.Amount))
		return nil
	})
	return err
}

// Verify checks if the account balances in Redis match the expected values.
func (r *RedisParticipant) Verify(ctx context.Context, req TransferRequest, snapshot *Snapshot) (bool, error) {
	currentFromBalance, err := r.rdb.Get(ctx, req.From).Int()
	if err != nil {
		return false, fmt.Errorf("failed to verify from balance: %w", err)
	}
	currentToBalance, err := r.rdb.Get(ctx, req.To).Int()
	if err != nil {
		return false, fmt.Errorf("failed to verify to balance: %w", err)
	}
	expectedFromBalance := snapshot.Data["FromBalance"].(int) - req.Amount
	expectedToBalance := snapshot.Data["ToBalance"].(int) + req.Amount
	return currentFromBalance == expectedFromBalance && currentToBalance == expectedToBalance, nil
}

// MysqlParticipant implements the Participant interface for MySQL.
type MysqlParticipant struct {
	db *sql.DB
}

// NewMysqlParticipant creates a new MysqlParticipant instance.
func NewMysqlParticipant(db *sql.DB) *MysqlParticipant {
	return &MysqlParticipant{
		db: db,
	}
}

// Prepare checks the balance in MySQL and creates a snapshot.
func (m *MysqlParticipant) Prepare(ctx context.Context, req TransferRequest) (*Snapshot, error) {
	var fromBalance, toBalance int
	if err := m.db.QueryRowContext(ctx, "SELECT balance FROM accounts WHERE account_id =?", req.From).Scan(&fromBalance); err != nil {
		return nil, fmt.Errorf("failed to query balance: %w", err)
	}
	if fromBalance < req.Amount {
		return nil, errors.New("insufficient balance")
	}
	if err := m.db.QueryRowContext(ctx, "SELECT balance FROM accounts WHERE account_id =?", req.To).Scan(&toBalance); err != nil {
		return nil, fmt.Errorf("failed to query balance: %w", err)
	}
	return &Snapshot{
		Data: map[string]interface{}{
			"From":        req.From,
			"FromBalance": fromBalance,
			"To":          req.To,
			"ToBalance":   toBalance,
		},
	}, nil
}

// Commit performs database operations in a transaction.
func (m *MysqlParticipant) Commit(ctx context.Context, req TransferRequest) error {
	tx, err := m.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			tx.Rollback()
		}
	}()
	if _, err = tx.ExecContext(ctx, "UPDATE accounts SET balance = balance -? WHERE account_id =?", req.Amount, req.From); err != nil {
		return fmt.Errorf("failed to deduct balance: %w", err)
	}
	if _, err = tx.ExecContext(ctx, "UPDATE accounts SET balance = balance +? WHERE account_id =?", req.Amount, req.To); err != nil {
		return fmt.Errorf("failed to add balance: %w", err)
	}
	return tx.Commit()
}

// Rollback performs reverse database operations.
func (m *MysqlParticipant) Rollback(ctx context.Context, req TransferRequest) error {
	if _, err := m.db.ExecContext(ctx, "UPDATE accounts SET balance = balance +? WHERE account_id =?", req.Amount, req.From); err != nil {
		return fmt.Errorf("failed to restore sender's account: %w", err)
	}
	if _, err := m.db.ExecContext(ctx, "UPDATE accounts SET balance = balance -? WHERE account_id =?", req.Amount, req.To); err != nil {
		return fmt.Errorf("failed to restore receiver's account: %w", err)
	}
	return nil
}

// Verify checks if the account balances in MySQL match the expected values.
func (m *MysqlParticipant) Verify(ctx context.Context, req TransferRequest, snapshot *Snapshot) (bool, error) {
	var currentFromBalance, currentToBalance int
	err := m.db.QueryRowContext(ctx, "SELECT balance FROM accounts WHERE account_id =?", req.From).Scan(&currentFromBalance)
	if err != nil {
		return false, fmt.Errorf("failed to verify from balance: %w", err)
	}
	err = m.db.QueryRowContext(ctx, "SELECT balance FROM accounts WHERE account_id =?", req.To).Scan(&currentToBalance)
	if err != nil {
		return false, fmt.Errorf("failed to verify to balance: %w", err)
	}
	expectedFromBalance := snapshot.Data["FromBalance"].(int) - req.Amount
	expectedToBalance := snapshot.Data["ToBalance"].(int) + req.Amount
	return currentFromBalance == expectedFromBalance && currentToBalance == expectedToBalance, nil
}

// Coordinator coordinates the distributed transaction.
type Coordinator struct {
	participants []Participant
}

// Transfer executes a distributed transaction.
func (c *Coordinator) Transfer(ctx context.Context, req TransferRequest) error {
	// Phase 1: Prepare
	log.Printf("Transaction prepare phase started, XID: %s\n", req.Xid)
	var snapshots []*Snapshot
	for _, p := range c.participants {
		s, err := p.Prepare(ctx, req)
		if err != nil {
			log.Printf("Transaction prepare phase failed, XID: %s, Error: %v\n", req.Xid, err)
			return fmt.Errorf("prepare phase failed: %w", err)
		}
		snapshots = append(snapshots, s)
	}
	log.Printf("Transaction prepare phase succeeded, XID: %s\n", req.Xid)

	// Phase 2: Commit
	log.Printf("Transaction commit phase started, XID: %s\n", req.Xid)
	var pendingParticipants []int
	var failedParticipants []int
	var committedParticipants []int
	for i, p := range c.participants {
		if err := p.Commit(ctx, req); err != nil {
			if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
				pendingParticipants = append(pendingParticipants, i)
				continue
			}
			failedParticipants = append(failedParticipants, i)
			log.Printf("Transaction commit phase failed, XID: %s, Participant %d commit failed, Error: %v\n", req.Xid, i, err)
		} else {
			committedParticipants = append(committedParticipants, i)
			log.Printf("Transaction commit phase succeeded, XID: %s, Participant %d commit succeeded\n", req.Xid, i)
		}
	}

	// Transaction success
	if len(committedParticipants) == len(c.participants) {
		log.Printf("Transaction committed successfully, XID: %s\n", req.Xid)
		return nil
	}

	// Phase 3: Verify
	log.Printf("Transaction verification phase started, XID: %s\n", req.Xid)
	for i := 0; i < len(pendingParticipants); i++ {
		idx := pendingParticipants[i]
		ok, err := c.participants[idx].Verify(ctx, req, snapshots[idx])
		if err != nil {
			log.Printf("Transaction verification phase failed, XID: %s, Participant %d verification failed, Error: %v\n", req.Xid, idx, err)
			return fmt.Errorf("prepare phase failed: %w", err)
		}
		if ok {
			committedParticipants = append(committedParticipants, idx)
			log.Printf("Transaction verification phase succeeded, XID: %s, Participant %d verification succeeded\n", req.Xid, idx)
		} else {
			failedParticipants = append(failedParticipants, idx)
			log.Printf("Transaction verification phase failed, XID: %s, Participant %d verification failed\n", req.Xid, idx)
		}
	}

	// Phase 4: Rollback
	if len(failedParticipants) > 0 {
		log.Printf("Transaction rollback phase started, XID: %s\n", req.Xid)
		var rollbackErrors []error
		for _, idx := range committedParticipants {
			if err := c.participants[idx].Rollback(ctx, req); err != nil {
				rollbackErrors = append(rollbackErrors, fmt.Errorf("participant %d rollback failed: %w", idx, err))
				log.Printf("Transaction rollback phase failed, XID: %s, Participant %d rollback failed, Error: %v\n", req.Xid, idx, err)
			} else {
				log.Printf("Transaction rollback phase succeeded, XID: %s, Participant %d rollback succeeded\n", req.Xid, idx)
			}
		}
		if len(rollbackErrors) > 0 {
			log.Printf("Transaction failed and rollback had errors, XID: %s, Errors: %v\n", req.Xid, rollbackErrors)
			return fmt.Errorf("transaction failed, rollback errors: %v", rollbackErrors)
		}
		log.Printf("Transaction failed but successfully rolled back, XID: %s\n", req.Xid)
	}
	log.Printf("Transaction rolled back, XID: %s\n", req.Xid)
	return errors.New("rolled back")
}
