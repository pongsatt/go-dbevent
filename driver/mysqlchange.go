package driver

import (
	"sync"
	"time"

	mysql "github.com/go-sql-driver/mysql"
	"github.com/pongsatt/go-dbevent"
	"github.com/siddontang/go-mysql/canal"
)

// MySQLChange represents event change listener
type MySQLChange struct {
	canal.DummyEventHandler
	canal          *canal.Canal
	tableName      string
	waitLock       sync.Mutex
	waitChangeChan chan bool
	runningLock    sync.Mutex
	running        bool
}

// NewMySQLChange creates new instance
func NewMySQLChange(config *dbevent.DBConfig, tableName string) *MySQLChange {
	mysqlConfig, err := mysql.ParseDSN(config.ToDSN())

	cfg := canal.NewDefaultConfig()
	cfg.Addr = mysqlConfig.Addr
	cfg.User = mysqlConfig.User
	cfg.Password = mysqlConfig.Passwd
	cfg.Dump.ExecutionPath = "" // do not use mysqldump

	c, err := canal.NewCanal(cfg)

	if err != nil {
		panic(err)
	}

	change := &MySQLChange{canal: c, tableName: tableName}
	c.SetEventHandler(change)

	return change
}

// OnRow receives change event
func (h *MySQLChange) OnRow(e *canal.RowsEvent) error {
	if e.Table.Name == h.tableName {
		// fmt.Printf("%s table: %s\n", e.Action, e.Table.Name)

		h.waitLock.Lock()
		if h.waitChangeChan != nil {
			h.waitChangeChan <- true
			h.waitChangeChan = nil
		}
		h.waitLock.Unlock()
	}
	return nil
}

// WaitChange waits for change
func (h *MySQLChange) WaitChange(timeout time.Duration) {
	h.Run()

	h.waitLock.Lock()
	h.waitChangeChan = make(chan bool)
	h.waitLock.Unlock()

	select {
	case <-h.waitChangeChan:
	case <-time.After(timeout):
	}
}

func (h *MySQLChange) String() string {
	return "Change"
}

// Run starts listening
func (h *MySQLChange) Run() {
	h.runningLock.Lock()
	defer h.runningLock.Unlock()

	if h.running {
		return
	}

	h.running = true

	go func() {
		pos, err := h.canal.GetMasterPos()

		if err != nil {
			return
		}

		if err = h.canal.RunFrom(pos); err != nil {
			h.running = false
		}
	}()
}

// Close stop listening
func (h *MySQLChange) Close() {
	h.canal.Close()
}
