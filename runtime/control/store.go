package control

import (
	"strings"

	ctl "github.com/punk-one/edge-service-sdk/control"
)

// JobState tracks one local control task by trace id.
type JobState struct {
	TraceID     string
	DeviceCode  string
	ProductCode string
	Kind        string
	Identifier  string
	Code        int
	Message     string
	CreatedAt   int64
	UpdatedAt   int64
	FinishedAt  int64
}

// JobFilter scopes a local control-job list query.
type JobFilter struct {
	DeviceCode  string
	Kind        string
	Identifier  string
	FinalSet    bool
	Final       bool
	Limit       int
	Offset      int
	CreatedFrom int64
	CreatedTo   int64
	UpdatedFrom int64
	UpdatedTo   int64
}

// JobDiagnostics summarizes local control-job health and progress.
type JobDiagnostics struct {
	Total                int
	Pending              int
	Processing           int
	Accepted             int
	Final                int
	Success              int
	PartialSuccess       int
	Failed               int
	Property             int
	Command              int
	LatestUpdatedAt      int64
	PendingCommandQueue  int
	PendingPropertyQueue int
}

// PendingCommand stores one async command that still needs to complete.
type PendingCommand struct {
	TraceID     string
	DeviceCode  string
	ProductCode string
	Identifier  string
	Request     ctl.Request
	CreatedAt   int64
	UpdatedAt   int64
}

// PendingProperty stores one async property request that still needs to complete.
type PendingProperty struct {
	TraceID     string
	DeviceCode  string
	ProductCode string
	Operation   string
	Request     ctl.Request
	CreatedAt   int64
	UpdatedAt   int64
}

// Store persists local control jobs, recorded results, and pending async work.
type Store interface {
	LoadJob(traceID string) (JobState, bool, error)
	ListJobs(filter JobFilter) ([]JobState, error)
	JobDiagnostics(filter JobFilter) (JobDiagnostics, error)
	LoadLatestResult(traceID string) (ctl.Result, bool, error)
	ListResults(traceID string, limit int) ([]ctl.Result, error)
	UpsertJob(job JobState) (bool, error)
	SaveResult(traceID string, result ctl.Result, final bool) error
	SavePendingCommand(job PendingCommand) (bool, error)
	DeletePendingCommand(traceID string) error
	ListPendingCommands() ([]PendingCommand, error)
	SavePendingProperty(job PendingProperty) (bool, error)
	DeletePendingProperty(traceID string) error
	ListPendingProperties() ([]PendingProperty, error)
	Close() error
}

func IsFinalCode(code int) bool {
	switch code {
	case ctl.CodeProcessing, ctl.CodeAccepted:
		return false
	default:
		return true
	}
}

func NormalizeKind(kind string, identifier string) string {
	kind = strings.TrimSpace(kind)
	identifier = strings.TrimSpace(identifier)
	if kind == "" {
		kind = "command"
	}
	if identifier == "" || strings.Contains(kind, ":") {
		return kind
	}
	return kind + ":" + identifier
}
