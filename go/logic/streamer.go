/*
   Copyright 2022 GitHub Inc.
	 See https://github.com/github/gh-ost/blob/master/LICENSE
*/

package logic

import (
	gosql "database/sql"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/github/gh-ost/go/base"
	"github.com/github/gh-ost/go/binlog"
	"github.com/github/gh-ost/go/mysql"

	"github.com/openark/golib/sqlutils"
)

type BinlogEventListener struct {
	async        bool
	databaseName string
	tableName    string
	onDmlEvent   func(event *binlog.BinlogDMLEvent) error
}

const (
	EventsChannelBufferSize       = 1
	ReconnectStreamerSleepSeconds = 5
)

// EventsStreamer reads data from binary logs and streams it on. It acts as a publisher,
// and interested parties may subscribe for per-table events.
// EventsStreamer 从二进制日志中读取数据并将其流式传输。
type EventsStreamer struct {
	connectionConfig         *mysql.ConnectionConfig
	db                       *gosql.DB
	migrationContext         *base.MigrationContext
	initialBinlogCoordinates *mysql.BinlogCoordinates
	listeners                [](*BinlogEventListener)
	listenersMutex           *sync.Mutex
	eventsChannel            chan *binlog.BinlogEntry
	binlogReader             *binlog.GoMySQLReader
	name                     string
}

func NewEventsStreamer(migrationContext *base.MigrationContext) *EventsStreamer {
	return &EventsStreamer{
		connectionConfig: migrationContext.InspectorConnectionConfig,
		migrationContext: migrationContext,
		listeners:        [](*BinlogEventListener){},
		listenersMutex:   &sync.Mutex{},
		eventsChannel:    make(chan *binlog.BinlogEntry, EventsChannelBufferSize),
		name:             "streamer",
	}
}

// AddListener registers a new listener for binlog events, on a per-table basis
// AddListener 为 binlog 事件注册一个新的监听器，基于per-table的配置
func (this *EventsStreamer) AddListener(
	async bool, databaseName string, tableName string, onDmlEvent func(event *binlog.BinlogDMLEvent) error) (err error) {
	// 加锁
	this.listenersMutex.Lock()
	// 解锁
	defer this.listenersMutex.Unlock()

	// 数据库名称 和 changelog 名称为空则报错
	if databaseName == "" {
		return fmt.Errorf("Empty database name in AddListener")
	}
	if tableName == "" {
		return fmt.Errorf("Empty table name in AddListener")
	}
	listener := &BinlogEventListener{
		async:        async,
		databaseName: databaseName,
		tableName:    tableName,
		onDmlEvent:   onDmlEvent,
	}
	this.listeners = append(this.listeners, listener)
	return nil
}

// notifyListeners will notify relevant listeners with given DML event. Only
// listeners registered for changes on the table on which the DML operates are notified.
func (this *EventsStreamer) notifyListeners(binlogEvent *binlog.BinlogDMLEvent) {
	this.listenersMutex.Lock()
	defer this.listenersMutex.Unlock()

	for _, listener := range this.listeners {
		listener := listener
		if !strings.EqualFold(listener.databaseName, binlogEvent.DatabaseName) {
			continue
		}
		if !strings.EqualFold(listener.tableName, binlogEvent.TableName) {
			continue
		}
		if listener.async {
			go func() {
				listener.onDmlEvent(binlogEvent)
			}()
		} else {
			listener.onDmlEvent(binlogEvent)
		}
	}
}

func (this *EventsStreamer) InitDBConnections() (err error) {
	// 初始化数据库连接
	EventsStreamerUri := this.connectionConfig.GetDBUri(this.migrationContext.DatabaseName)
	if this.db, _, err = mysql.GetDB(this.migrationContext.Uuid, EventsStreamerUri); err != nil {
		return err
	}
	// 验证MySQL连接
	if _, err := base.ValidateConnection(this.db, this.connectionConfig, this.migrationContext, this.name); err != nil {
		return err
	}
	// 获取binlog的位点
	if err := this.readCurrentBinlogCoordinates(); err != nil {
		return err
	}
	if err := this.initBinlogReader(this.initialBinlogCoordinates); err != nil {
		return err
	}

	return nil
}

// initBinlogReader creates and connects the reader: we hook up to a MySQL server as a replica
// initBinlogReader 创建并连接读取器：我们伪装成为MySQL服务器的一个从库。
func (this *EventsStreamer) initBinlogReader(binlogCoordinates *mysql.BinlogCoordinates) error {
	goMySQLReader := binlog.NewGoMySQLReader(this.migrationContext)
	if err := goMySQLReader.ConnectBinlogStreamer(*binlogCoordinates); err != nil {
		return err
	}
	this.binlogReader = goMySQLReader
	return nil
}

func (this *EventsStreamer) GetCurrentBinlogCoordinates() *mysql.BinlogCoordinates {
	return this.binlogReader.GetCurrentBinlogCoordinates()
}

func (this *EventsStreamer) GetReconnectBinlogCoordinates() *mysql.BinlogCoordinates {
	return &mysql.BinlogCoordinates{LogFile: this.GetCurrentBinlogCoordinates().LogFile, LogPos: 4}
}

// readCurrentBinlogCoordinates reads master status from hooked server
func (this *EventsStreamer) readCurrentBinlogCoordinates() error {
	query := `show /* gh-ost readCurrentBinlogCoordinates */ master status`
	foundMasterStatus := false
	err := sqlutils.QueryRowsMap(this.db, query, func(m sqlutils.RowMap) error {
		this.initialBinlogCoordinates = &mysql.BinlogCoordinates{
			LogFile: m.GetString("File"),
			LogPos:  m.GetInt64("Position"),
		}
		foundMasterStatus = true

		return nil
	})
	if err != nil {
		return err
	}
	if !foundMasterStatus {
		return fmt.Errorf("Got no results from SHOW MASTER STATUS. Bailing out")
	}
	this.migrationContext.Log.Debugf("Streamer binlog coordinates: %+v", *this.initialBinlogCoordinates)
	return nil
}

// StreamEvents will begin streaming events. It will be blocking, so should be
// executed by a goroutine
func (this *EventsStreamer) StreamEvents(canStopStreaming func() bool) error {
	go func() {
		for binlogEntry := range this.eventsChannel {
			if binlogEntry.DmlEvent != nil {
				this.notifyListeners(binlogEntry.DmlEvent)
			}
		}
	}()
	// The next should block and execute forever, unless there's a serious error
	var successiveFailures int64
	var lastAppliedRowsEventHint mysql.BinlogCoordinates
	for {
		if canStopStreaming() {
			return nil
		}
		if err := this.binlogReader.StreamEvents(canStopStreaming, this.eventsChannel); err != nil {
			if canStopStreaming() {
				return nil
			}

			this.migrationContext.Log.Infof("StreamEvents encountered unexpected error: %+v", err)
			this.migrationContext.MarkPointOfInterest()
			time.Sleep(ReconnectStreamerSleepSeconds * time.Second)

			// See if there's retry overflow
			if this.binlogReader.LastAppliedRowsEventHint.Equals(&lastAppliedRowsEventHint) {
				successiveFailures += 1
			} else {
				successiveFailures = 0
			}
			if successiveFailures > this.migrationContext.MaxRetries() {
				return fmt.Errorf("%d successive failures in streamer reconnect at coordinates %+v", successiveFailures, this.GetReconnectBinlogCoordinates())
			}

			// Reposition at same binlog file.
			lastAppliedRowsEventHint = this.binlogReader.LastAppliedRowsEventHint
			this.migrationContext.Log.Infof("Reconnecting... Will resume at %+v", lastAppliedRowsEventHint)
			if err := this.initBinlogReader(this.GetReconnectBinlogCoordinates()); err != nil {
				return err
			}
			this.binlogReader.LastAppliedRowsEventHint = lastAppliedRowsEventHint
		}
	}
}

func (this *EventsStreamer) Close() (err error) {
	err = this.binlogReader.Close()
	this.migrationContext.Log.Infof("Closed streamer connection. err=%+v", err)
	return err
}

func (this *EventsStreamer) Teardown() {
	this.db.Close()
}
