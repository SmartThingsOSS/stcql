package stcql

import (
	"fmt"
	"sync"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/gocql/gocql"
)

const CQLTimeFmt = "2006-01-02 15:04:05-0700"

// Mutex represents a mutex that is acquired by inserting a row
// into a Cassandra table using a lightweight transaction.
type Mutex struct {
	Session    *gocql.Session
	Keyspace   string
	Table      string
	LockName   string
	LockHolder string
	// Lifetime is the duration that the mutex is valid after it's acquired.
	// It is considered stale after the duration has passed.
	Lifetime time.Duration
	// RetryTime is the duration between attempts to acquire the mutex
	RetryTime time.Duration
	// Timeout is the duration after which to give up acquiring the mutex
	Timeout time.Duration
}

// Lock attempts to acquire mutex m. If the lock is already in use, the calling
// goroutine blocks until the mutex is available
func (m *Mutex) Lock() error {
	l, err := m.tryLock()
	if l || err != nil {
		return err
	}
	// if RetryTime is not a positive value, we don't want to retry. Just fail instead.
	if m.RetryTime <= 0 {
		return fmt.Errorf("failed to acquire lock '%s'", m.LockName)
	}
	retryTicker := time.Tick(m.RetryTime)
	var timeoutTicker <-chan time.Time
	if m.Timeout > 0 {
		timeoutTicker = time.Tick(m.Timeout)
	} else {
		// if m.Timeout isn't a positive value, just make an unconnected channel to poll
		// (effectively making the timeout infinite)
		timeoutTicker = make(chan time.Time)
	}
	for {
		select {
		case <-retryTicker:
			l, err := m.tryLock()
			if l || err != nil {
				return err
			}
		case <-timeoutTicker:
			return fmt.Errorf("mutex lock hit timeout of %v", m.Timeout)
		}
	}
}

func (m *Mutex) tryLock() (bool, error) {
	now := time.Now()
	nowString := now.Format(CQLTimeFmt)
	var expiration string
	if m.Lifetime > 0 {
		expiration = now.Add(m.Lifetime).Format(CQLTimeFmt)
	}
	queryString := fmt.Sprintf(
		"INSERT INTO \"%s\".\"%s\" (lock_name, acquired_time, expiration_time, lock_holder) VALUES ('%s', '%s', '%s', '%s') IF NOT EXISTS",
		m.Keyspace,
		m.Table,
		m.LockName,
		nowString,
		expiration,
		m.LockHolder,
	)
	log.Debugf("trying locking query string: %s", queryString)
	data := map[string]interface{}{}
	applied, err := m.Session.Query(queryString).MapScanCAS(data)
	if err != nil {
		return false, err
	}
	log.Debugf("query applied status: %v", applied)
	if applied {
		log.Debugf("locked mutex '%s' as '%s'", m.LockName, m.LockHolder)
	} else {
		// check for stale lock, or pre-existing lock with the same holder name
		lockExpiration := data["expiration_time"].(time.Time)
		if (!lockExpiration.IsZero() && lockExpiration.Before(now)) || data["lock_holder"].(string) == m.LockHolder {
			// existing lock is stale
			queryString := fmt.Sprintf(
				"UPDATE \"%s\".\"%s\" SET lock_holder = '%s', acquired_time = '%s', expiration_time = '%s' WHERE lock_name = '%s' IF acquired_time = '%s'",
				m.Keyspace,
				m.Table,
				m.LockHolder,
				nowString,
				expiration,
				m.LockName,
				data["acquired_time"].(time.Time).Format(CQLTimeFmt),
			)
			log.Debugf("trying locking query string: %s", queryString)
			data = map[string]interface{}{}
			applied, err = m.Session.Query(queryString).MapScanCAS(data)
			if err != nil {
				return false, err
			}
			if applied {
				log.Debugf("locked stale mutex '%s' as '%s'", m.LockName, m.LockHolder)
			} else {
				log.Debugf("failed to acquire stale mutex '%s'", m.LockName)
			}
		} else {
			// existing lock is not stale
			log.Debugf("failed to lock mutex '%s' as it's already held by '%s'", m.LockName, data["lock_holder"].(string))
		}
	}
	return applied, nil
}

// Unlock unlocks m. Unlock returns an error if m is not locked on entry.
func (m *Mutex) Unlock() error {
	queryString := fmt.Sprintf(
		"DELETE FROM \"%s\".\"%s\"  WHERE lock_name = '%s' IF lock_holder = '%s'",
		m.Keyspace,
		m.Table,
		m.LockName,
		m.LockHolder,
	)
	log.Debugf("trying unlock query string: %s", queryString)
	data := map[string]interface{}{}
	applied, err := m.Session.Query(queryString).MapScanCAS(data)
	if err != nil {
		return err
	}
	if !applied {
		return fmt.Errorf("Failed to unlock mutex: '%s'\nC* returned: '%v'\n", m.LockName, data)
	}
	log.Debugf("unlocked mutex '%s' as '%s'", m.LockName, m.LockHolder)
	return nil
}

// Locker returns a sync.Locker interface for Mutex
func (m *Mutex) Locker() sync.Locker {
	return (*cqllocker)(m)
}

type cqllocker Mutex

func (m *cqllocker) Lock() {
	if err := (*Mutex)(m).Lock(); err != nil {
		panic(err)
	}
}

func (m *cqllocker) Unlock() {
	if err := (*Mutex)(m).Unlock(); err != nil {
		panic(err)
	}
}
