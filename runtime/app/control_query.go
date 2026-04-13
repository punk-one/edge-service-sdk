package app

import (
	"fmt"
	"net/http"
	"sort"
	"strings"
	"time"

	cmdapi "github.com/punk-one/edge-service-sdk/command"
	cfg "github.com/punk-one/edge-service-sdk/config"
	ctl "github.com/punk-one/edge-service-sdk/control"
	contracts "github.com/punk-one/edge-service-sdk/driver"
	httpserver "github.com/punk-one/edge-service-sdk/ops/http"
	rtcontrol "github.com/punk-one/edge-service-sdk/runtime/control"
)

func buildPropertyModelQuery(sdk *DeviceSDK) func(string) (map[string]interface{}, int) {
	return func(deviceCode string) (map[string]interface{}, int) {
		device, ok := lookupDeviceConfig(sdk, deviceCode)
		if !ok {
			return errorBody(http.StatusNotFound, "device not found"), http.StatusNotFound
		}
		return map[string]interface{}{
			"device_code":        device.Name,
			"product_code":       device.ProductCode,
			"interval":           strings.TrimSpace(device.Property.Interval),
			"on_change":          device.Property.OnChange,
			"watched_fields":     cloneStrings(device.Property.WatchedFields),
			"heartbeat_interval": strings.TrimSpace(device.Property.HeartbeatInterval),
			"points":             propertyPointsView(device.Property.Points),
			"structs":            propertyStructsView(device.Property.Structs),
		}, http.StatusOK
	}
}

func buildTelemetryModelQuery(sdk *DeviceSDK) func(string) (map[string]interface{}, int) {
	return func(deviceCode string) (map[string]interface{}, int) {
		device, ok := lookupDeviceConfig(sdk, deviceCode)
		if !ok {
			return errorBody(http.StatusNotFound, "device not found"), http.StatusNotFound
		}
		return map[string]interface{}{
			"device_code":        device.Name,
			"product_code":       device.ProductCode,
			"interval":           strings.TrimSpace(device.Telemetry.Interval),
			"on_change":          device.Telemetry.OnChange,
			"watched_fields":     cloneStrings(device.Telemetry.WatchedFields),
			"heartbeat_interval": strings.TrimSpace(device.Telemetry.HeartbeatInterval),
			"points":             pointViews(device.Telemetry.Points),
		}, http.StatusOK
	}
}

func buildCommandListQuery(sdk *DeviceSDK, registry cmdapi.Registry) func(string) (map[string]interface{}, int) {
	return func(deviceCode string) (map[string]interface{}, int) {
		device, ok := lookupDeviceConfig(sdk, deviceCode)
		if !ok {
			return errorBody(http.StatusNotFound, "device not found"), http.StatusNotFound
		}
		descriptors := listDeviceCommands(device, registry)
		commands := make([]map[string]interface{}, 0, len(descriptors))
		for _, descriptor := range descriptors {
			commands = append(commands, commandSummaryView(descriptor))
		}
		return map[string]interface{}{
			"device_code":  device.Name,
			"product_code": device.ProductCode,
			"commands":     commands,
		}, http.StatusOK
	}
}

func buildCommandDetailQuery(sdk *DeviceSDK, registry cmdapi.Registry) func(string, string) (map[string]interface{}, int) {
	return func(deviceCode string, identifier string) (map[string]interface{}, int) {
		device, ok := lookupDeviceConfig(sdk, deviceCode)
		if !ok {
			return errorBody(http.StatusNotFound, "device not found"), http.StatusNotFound
		}
		command, ok := lookupDeviceCommand(device, registry, identifier)
		if !ok {
			return errorBody(http.StatusNotFound, "command not found"), http.StatusNotFound
		}
		return map[string]interface{}{
			"device_code":   device.Name,
			"product_code":  device.ProductCode,
			"identifier":    command.Identifier,
			"name":          command.Name,
			"mode":          normalizedCommandMode(command.Mode),
			"input_params":  commandParamViews(command.InputParams),
			"output_params": commandParamViews(command.OutputParams),
		}, http.StatusOK
	}
}

func buildCommandInputQuery(sdk *DeviceSDK, registry cmdapi.Registry) func(string, string) (map[string]interface{}, int) {
	return func(deviceCode string, identifier string) (map[string]interface{}, int) {
		device, ok := lookupDeviceConfig(sdk, deviceCode)
		if !ok {
			return errorBody(http.StatusNotFound, "device not found"), http.StatusNotFound
		}
		command, ok := lookupDeviceCommand(device, registry, identifier)
		if !ok {
			return errorBody(http.StatusNotFound, "command not found"), http.StatusNotFound
		}
		return map[string]interface{}{
			"device_code":  device.Name,
			"product_code": device.ProductCode,
			"identifier":   command.Identifier,
			"name":         command.Name,
			"mode":         normalizedCommandMode(command.Mode),
			"input_params": commandParamViews(command.InputParams),
		}, http.StatusOK
	}
}

func buildCommandOutputQuery(sdk *DeviceSDK, registry cmdapi.Registry) func(string, string) (map[string]interface{}, int) {
	return func(deviceCode string, identifier string) (map[string]interface{}, int) {
		device, ok := lookupDeviceConfig(sdk, deviceCode)
		if !ok {
			return errorBody(http.StatusNotFound, "device not found"), http.StatusNotFound
		}
		command, ok := lookupDeviceCommand(device, registry, identifier)
		if !ok {
			return errorBody(http.StatusNotFound, "command not found"), http.StatusNotFound
		}
		return map[string]interface{}{
			"device_code":   device.Name,
			"product_code":  device.ProductCode,
			"identifier":    command.Identifier,
			"name":          command.Name,
			"mode":          normalizedCommandMode(command.Mode),
			"output_params": commandParamViews(command.OutputParams),
		}, http.StatusOK
	}
}

func buildControlJobListQuery(store rtcontrol.Store) func(httpserver.ControlJobListQuery) (map[string]interface{}, int) {
	return func(query httpserver.ControlJobListQuery) (map[string]interface{}, int) {
		if store == nil {
			return errorBody(http.StatusServiceUnavailable, "control job store is unavailable"), http.StatusServiceUnavailable
		}
		limit := normalizedJobLimit(query.Limit)
		offset := normalizedJobOffset(query.Offset)
		filter := rtcontrol.JobFilter{
			DeviceCode:  query.DeviceCode,
			Kind:        query.Kind,
			Identifier:  query.Identifier,
			FinalSet:    query.FinalSet,
			Final:       query.Final,
			Limit:       limit,
			Offset:      offset,
			CreatedFrom: query.CreatedFrom,
			CreatedTo:   query.CreatedTo,
			UpdatedFrom: query.UpdatedFrom,
			UpdatedTo:   query.UpdatedTo,
		}
		items, err := store.ListJobs(filter)
		if err != nil {
			return errorBody(http.StatusInternalServerError, err.Error()), http.StatusInternalServerError
		}
		jobs := make([]map[string]interface{}, 0, len(items))
		for _, item := range items {
			jobs = append(jobs, jobStateView(item))
		}
		return map[string]interface{}{
			"jobs":  jobs,
			"query": controlJobQueryView(query, limit, offset),
			"paging": map[string]interface{}{
				"limit":    limit,
				"offset":   offset,
				"returned": len(jobs),
			},
		}, http.StatusOK
	}
}

func buildControlJobExportQuery(store rtcontrol.Store) func(httpserver.ControlJobExportQuery) (map[string]interface{}, int) {
	return func(query httpserver.ControlJobExportQuery) (map[string]interface{}, int) {
		if store == nil {
			return errorBody(http.StatusServiceUnavailable, "control job export is unavailable"), http.StatusServiceUnavailable
		}
		limit := normalizedExportLimit(query.Limit)
		offset := normalizedJobOffset(query.Offset)
		filter := rtcontrol.JobFilter{
			DeviceCode:  query.DeviceCode,
			Kind:        query.Kind,
			Identifier:  query.Identifier,
			FinalSet:    query.FinalSet,
			Final:       query.Final,
			Limit:       limit,
			Offset:      offset,
			CreatedFrom: query.CreatedFrom,
			CreatedTo:   query.CreatedTo,
			UpdatedFrom: query.UpdatedFrom,
			UpdatedTo:   query.UpdatedTo,
		}
		items, err := store.ListJobs(filter)
		if err != nil {
			return errorBody(http.StatusInternalServerError, err.Error()), http.StatusInternalServerError
		}
		rows := make([]map[string]interface{}, 0, len(items))
		for _, item := range items {
			row := jobStateView(item)
			row["has_result"] = false
			if latest, foundResult, err := store.LoadLatestResult(item.TraceID); err == nil && foundResult {
				row["has_result"] = true
				row["latest_result_code"] = latest.Code
				row["latest_result_message"] = latest.Message
				row["latest_result_time"] = latest.Time
				row["latest_result_final"] = rtcontrol.IsFinalCode(latest.Code)
			}
			if summary, err := controlJobTimelineSummary(store, item); err == nil {
				row["event_count"] = summary.EventCount
				row["last_event_at"] = summary.LastEventAt
				row["last_progress_at"] = summary.LastProgressAt
				row["total_duration_ms"] = summary.TotalDurationMs
				if summary.LatestProgress != nil {
					row["latest_progress"] = summary.LatestProgress
				}
			}
			rows = append(rows, row)
		}
		columns := []string{
			"trace_id", "device_code", "product_code", "kind", "root_kind", "identifier",
			"code", "message", "status_phase", "final", "created_at", "updated_at", "finished_at", "duration_ms",
			"has_result", "latest_result_code", "latest_result_message", "latest_result_time", "latest_result_final",
			"event_count", "last_event_at", "last_progress_at", "total_duration_ms", "latest_progress",
		}
		filename := fmt.Sprintf("control_jobs_export_%s.%s", time.Now().Format("20060102_150405"), exportFilenameExt(query.Format))
		return map[string]interface{}{
			"format":   query.Format,
			"filename": filename,
			"columns":  columns,
			"rows":     rows,
			"query":    controlJobQueryView(query.ControlJobListQuery, limit, offset),
			"export": map[string]interface{}{
				"limit":    limit,
				"offset":   offset,
				"exported": len(rows),
			},
		}, http.StatusOK
	}
}

func buildControlJobDiagnosticsQuery(store rtcontrol.Store) func(httpserver.ControlJobListQuery) (map[string]interface{}, int) {
	return func(query httpserver.ControlJobListQuery) (map[string]interface{}, int) {
		if store == nil {
			return errorBody(http.StatusServiceUnavailable, "control job diagnostics is unavailable"), http.StatusServiceUnavailable
		}
		filter := rtcontrol.JobFilter{
			DeviceCode:  query.DeviceCode,
			Kind:        query.Kind,
			Identifier:  query.Identifier,
			FinalSet:    query.FinalSet,
			Final:       query.Final,
			CreatedFrom: query.CreatedFrom,
			CreatedTo:   query.CreatedTo,
			UpdatedFrom: query.UpdatedFrom,
			UpdatedTo:   query.UpdatedTo,
		}
		diagnostics, err := store.JobDiagnostics(filter)
		if err != nil {
			return errorBody(http.StatusInternalServerError, err.Error()), http.StatusInternalServerError
		}
		items, err := store.ListJobs(filter)
		if err != nil {
			return errorBody(http.StatusInternalServerError, err.Error()), http.StatusInternalServerError
		}
		return map[string]interface{}{
			"query": controlJobQueryView(query, normalizedJobLimit(query.Limit), normalizedJobOffset(query.Offset)),
			"summary": map[string]interface{}{
				"total":             diagnostics.Total,
				"pending":           diagnostics.Pending,
				"processing":        diagnostics.Processing,
				"accepted":          diagnostics.Accepted,
				"final":             diagnostics.Final,
				"success":           diagnostics.Success,
				"partial_success":   diagnostics.PartialSuccess,
				"failed":            diagnostics.Failed,
				"property":          diagnostics.Property,
				"command":           diagnostics.Command,
				"latest_updated_at": diagnostics.LatestUpdatedAt,
			},
			"pending_queue": map[string]interface{}{
				"command":  diagnostics.PendingCommandQueue,
				"property": diagnostics.PendingPropertyQueue,
			},
			"breakdown": controlJobDiagnosticsBreakdown(items),
		}, http.StatusOK
	}
}

func buildControlJobQuery(store rtcontrol.Store) func(string) (map[string]interface{}, int) {
	return func(traceID string) (map[string]interface{}, int) {
		if store == nil {
			return errorBody(http.StatusServiceUnavailable, "control job store is unavailable"), http.StatusServiceUnavailable
		}
		job, found, err := store.LoadJob(strings.TrimSpace(traceID))
		if err != nil {
			return errorBody(http.StatusInternalServerError, err.Error()), http.StatusInternalServerError
		}
		if !found {
			return errorBody(http.StatusNotFound, "control job not found"), http.StatusNotFound
		}
		body := jobStateView(job)
		body["status_phase"] = jobStatusPhase(job.Code)
		if latest, foundResult, err := store.LoadLatestResult(job.TraceID); err == nil && foundResult {
			body["has_result"] = true
			body["latest_result"] = map[string]interface{}{
				"trace_id": latest.TraceID,
				"code":     latest.Code,
				"message":  latest.Message,
				"time":     latest.Time,
				"data":     ensureDataMap(latest.Data),
				"final":    rtcontrol.IsFinalCode(latest.Code),
			}
		} else {
			body["has_result"] = false
		}
		if summary, err := controlJobTimelineSummary(store, job); err == nil {
			body["event_count"] = summary.EventCount
			body["last_event_at"] = summary.LastEventAt
			body["last_progress_at"] = summary.LastProgressAt
			body["total_duration_ms"] = summary.TotalDurationMs
			if summary.LatestProgress != nil {
				body["latest_progress"] = summary.LatestProgress
			}
		}
		return body, http.StatusOK
	}
}

func buildControlJobEventsQuery(store rtcontrol.Store) func(string, int) (map[string]interface{}, int) {
	return func(traceID string, limit int) (map[string]interface{}, int) {
		if store == nil {
			return errorBody(http.StatusServiceUnavailable, "control job events query is unavailable"), http.StatusServiceUnavailable
		}
		traceID = strings.TrimSpace(traceID)
		job, found, err := store.LoadJob(traceID)
		if err != nil {
			return errorBody(http.StatusInternalServerError, err.Error()), http.StatusInternalServerError
		}
		if !found {
			return errorBody(http.StatusNotFound, "control job not found"), http.StatusNotFound
		}
		limit = normalizedEventLimit(limit)
		items, err := store.ListResults(traceID, limit)
		if err != nil {
			return errorBody(http.StatusInternalServerError, err.Error()), http.StatusInternalServerError
		}
		events := make([]map[string]interface{}, 0, len(items))
		for index, item := range items {
			events = append(events, controlResultEventView(index+1, item))
		}
		summary, err := controlJobTimelineSummary(store, job)
		if err != nil {
			return errorBody(http.StatusInternalServerError, err.Error()), http.StatusInternalServerError
		}
		body := map[string]interface{}{
			"trace_id":          job.TraceID,
			"device_code":       job.DeviceCode,
			"kind":              job.Kind,
			"root_kind":         jobRootKind(job.Kind),
			"identifier":        job.Identifier,
			"returned":          len(events),
			"limit":             limit,
			"event_count":       summary.EventCount,
			"last_event_at":     summary.LastEventAt,
			"last_progress_at":  summary.LastProgressAt,
			"total_duration_ms": summary.TotalDurationMs,
			"events":            events,
		}
		if len(events) > 0 {
			body["latest_event"] = events[len(events)-1]
		}
		if summary.LatestProgress != nil {
			body["latest_progress"] = summary.LatestProgress
		}
		return body, http.StatusOK
	}
}

func buildControlJobResultQuery(store rtcontrol.Store) func(string) (map[string]interface{}, int) {
	return func(traceID string) (map[string]interface{}, int) {
		if store == nil {
			return errorBody(http.StatusServiceUnavailable, "control job store is unavailable"), http.StatusServiceUnavailable
		}
		traceID = strings.TrimSpace(traceID)
		result, found, err := store.LoadLatestResult(traceID)
		if err != nil {
			return errorBody(http.StatusInternalServerError, err.Error()), http.StatusInternalServerError
		}
		if found {
			if result.Data == nil {
				result.Data = map[string]interface{}{}
			}
			return map[string]interface{}{
				"trace_id": result.TraceID,
				"code":     result.Code,
				"message":  result.Message,
				"data":     result.Data,
				"time":     result.Time,
				"final":    rtcontrol.IsFinalCode(result.Code),
			}, http.StatusOK
		}
		job, found, err := store.LoadJob(traceID)
		if err != nil {
			return errorBody(http.StatusInternalServerError, err.Error()), http.StatusInternalServerError
		}
		if !found {
			return errorBody(http.StatusNotFound, "control job result not found"), http.StatusNotFound
		}
		timestamp := job.UpdatedAt
		if job.FinishedAt > 0 {
			timestamp = job.FinishedAt
		}
		return map[string]interface{}{
			"trace_id": job.TraceID,
			"code":     job.Code,
			"message":  job.Message,
			"data":     map[string]interface{}{},
			"time":     timestamp,
			"final":    rtcontrol.IsFinalCode(job.Code),
		}, http.StatusOK
	}
}

func buildPropertyResultQuery(store rtcontrol.Store) func(string) (map[string]interface{}, int) {
	return buildTypedControlResultQuery(store, "property")
}

func buildCommandResultQuery(store rtcontrol.Store) func(string) (map[string]interface{}, int) {
	return buildTypedControlResultQuery(store, "command")
}

func buildTypedControlResultQuery(store rtcontrol.Store, rootKind string) func(string) (map[string]interface{}, int) {
	return func(traceID string) (map[string]interface{}, int) {
		if store == nil {
			return errorBody(http.StatusServiceUnavailable, "control job store is unavailable"), http.StatusServiceUnavailable
		}
		traceID = strings.TrimSpace(traceID)
		job, found, err := store.LoadJob(traceID)
		if err != nil {
			return errorBody(http.StatusInternalServerError, err.Error()), http.StatusInternalServerError
		}
		if !found || jobRootKind(job.Kind) != rootKind {
			return errorBody(http.StatusNotFound, fmt.Sprintf("%s result not found", rootKind)), http.StatusNotFound
		}
		result, foundResult, err := store.LoadLatestResult(traceID)
		if err != nil {
			return errorBody(http.StatusInternalServerError, err.Error()), http.StatusInternalServerError
		}
		if foundResult {
			return map[string]interface{}{
				"trace_id": result.TraceID,
				"code":     result.Code,
				"message":  result.Message,
				"data":     ensureDataMap(result.Data),
				"time":     result.Time,
			}, http.StatusOK
		}
		timestamp := job.UpdatedAt
		if job.FinishedAt > 0 {
			timestamp = job.FinishedAt
		}
		return map[string]interface{}{
			"trace_id": job.TraceID,
			"code":     job.Code,
			"message":  job.Message,
			"data":     map[string]interface{}{},
			"time":     timestamp,
		}, http.StatusOK
	}
}

type controlTimelineSummary struct {
	EventCount      int
	LastEventAt     int64
	LastProgressAt  int64
	TotalDurationMs int64
	LatestProgress  map[string]interface{}
}

func controlJobTimelineSummary(store rtcontrol.Store, job rtcontrol.JobState) (controlTimelineSummary, error) {
	items, err := store.ListResults(job.TraceID, 0)
	if err != nil {
		return controlTimelineSummary{}, err
	}
	return summarizeControlResults(job, items), nil
}

func summarizeControlResults(job rtcontrol.JobState, items []ctl.Result) controlTimelineSummary {
	summary := controlTimelineSummary{
		EventCount: len(items),
	}
	lastEventAt := job.UpdatedAt
	if job.FinishedAt > 0 {
		lastEventAt = job.FinishedAt
	}
	for _, item := range items {
		if item.Time > lastEventAt {
			lastEventAt = item.Time
		}
		if progress := resultProgressView(item); progress != nil {
			summary.LastProgressAt = item.Time
			summary.LatestProgress = progress
		}
	}
	summary.LastEventAt = lastEventAt
	if job.CreatedAt > 0 && lastEventAt >= job.CreatedAt {
		summary.TotalDurationMs = lastEventAt - job.CreatedAt
	}
	return summary
}

func resultProgressView(result ctl.Result) map[string]interface{} {
	if len(result.Data) == 0 {
		return nil
	}
	progress, ok := result.Data["progress"].(map[string]interface{})
	if !ok || len(progress) == 0 {
		return nil
	}
	copy := make(map[string]interface{}, len(progress))
	for key, value := range progress {
		copy[key] = value
	}
	return copy
}

func lookupDeviceConfig(sdk *DeviceSDK, deviceCode string) (contracts.DeviceConfig, bool) {
	if sdk == nil {
		return contracts.DeviceConfig{}, false
	}
	device, ok := sdk.DeviceConfigByName(strings.TrimSpace(deviceCode))
	if !ok {
		return contracts.DeviceConfig{}, false
	}
	return device, true
}

func listDeviceCommands(device contracts.DeviceConfig, registry cmdapi.Registry) []cmdapi.CommandDescriptor {
	if registry == nil {
		return nil
	}
	items := make([]cmdapi.CommandDescriptor, 0, len(device.Commands))
	seen := map[string]struct{}{}
	for _, declared := range device.Commands {
		identifier := strings.TrimSpace(declared.Identifier)
		if identifier == "" {
			continue
		}
		if _, ok := seen[identifier]; ok {
			continue
		}
		seen[identifier] = struct{}{}
		descriptor, _, ok := registry.Lookup(identifier)
		if !ok {
			continue
		}
		items = append(items, descriptor)
	}
	return items
}

func lookupDeviceCommand(device contracts.DeviceConfig, registry cmdapi.Registry, identifier string) (cmdapi.CommandDescriptor, bool) {
	if _, ok := cfg.FindCommandByIdentifier(device, identifier); !ok {
		return cmdapi.CommandDescriptor{}, false
	}
	return lookupRegisteredCommand(registry, identifier)
}

func listRegisteredCommands(registry cmdapi.Registry) []cmdapi.CommandDescriptor {
	if registry == nil {
		return nil
	}
	return registry.List()
}

func lookupRegisteredCommand(registry cmdapi.Registry, identifier string) (cmdapi.CommandDescriptor, bool) {
	if registry == nil {
		return cmdapi.CommandDescriptor{}, false
	}
	descriptor, _, ok := registry.Lookup(strings.TrimSpace(identifier))
	if !ok {
		return cmdapi.CommandDescriptor{}, false
	}
	return descriptor, true
}

func pointViews(points []contracts.PointConfig) []map[string]interface{} {
	items := make([]map[string]interface{}, 0, len(points))
	for _, point := range points {
		items = append(items, map[string]interface{}{
			"name":               point.Name,
			"value_type":         point.ValueType,
			"node_name":          point.NodeName,
			"node_name_template": point.NodeNameTemplate,
			"array_key_pattern":  point.ArrayKeyPattern,
			"max_length":         point.MaxLength,
			"scale":              point.Scale,
			"precision":          point.Precision,
			"read_write":         point.ReadWrite,
			"on_change":          boolValue(point.OnChange),
			"deadband":           point.Deadband,
			"heartbeat_interval": point.HeartbeatInterval,
			"keep_latest_only":   point.KeepLatestOnly,
		})
	}
	return items
}

func propertyPointsView(points []contracts.PointConfig) []map[string]interface{} {
	return pointViews(points)
}

func propertyStructsView(structs []contracts.PropertyStruct) []map[string]interface{} {
	items := make([]map[string]interface{}, 0, len(structs))
	for _, item := range structs {
		fields := make([]map[string]interface{}, 0, len(item.Fields))
		for _, field := range item.Fields {
			fields = append(fields, map[string]interface{}{
				"name":         field.Name,
				"value_type":   field.ValueType,
				"field_offset": field.FieldOffset,
				"max_length":   field.MaxLength,
				"read_write":   field.ReadWrite,
			})
		}
		items = append(items, map[string]interface{}{
			"name":        item.Name,
			"kind":        item.Kind,
			"index_base":  item.IndexBase,
			"max_items":   item.MaxItems,
			"auto_report": item.AutoReport,
			"address": map[string]interface{}{
				"db_number":    item.Address.DBNumber,
				"base_offset":  item.Address.BaseOffset,
				"index_stride": item.Address.IndexStride,
				"unit":         item.Address.Unit,
			},
			"fields": fields,
		})
	}
	return items
}

func commandSummaryView(command cmdapi.CommandDescriptor) map[string]interface{} {
	return map[string]interface{}{
		"identifier":    command.Identifier,
		"name":          command.Name,
		"mode":          normalizedCommandMode(command.Mode),
		"input_params":  commandParamViews(command.InputParams),
		"output_params": commandParamViews(command.OutputParams),
	}
}

func commandParamViews(params []cmdapi.CommandParam) []map[string]interface{} {
	items := make([]map[string]interface{}, 0, len(params))
	for _, param := range params {
		items = append(items, map[string]interface{}{
			"identifier": param.Identifier,
			"value_type": param.ValueType,
			"required":   param.Required,
		})
	}
	return items
}

func boolValue(value *bool) interface{} {
	if value == nil {
		return nil
	}
	return *value
}

func cloneStrings(items []string) []string {
	if len(items) == 0 {
		return []string{}
	}
	return append([]string(nil), items...)
}

func cloneMap(items map[string]interface{}) map[string]interface{} {
	if len(items) == 0 {
		return map[string]interface{}{}
	}
	copy := make(map[string]interface{}, len(items))
	for key, value := range items {
		copy[key] = value
	}
	return copy
}

func normalizedCommandMode(mode string) string {
	mode = strings.TrimSpace(mode)
	if mode == "" {
		return "sync"
	}
	return mode
}

func errorBody(code int, message string) map[string]interface{} {
	return map[string]interface{}{
		"code":  code,
		"error": strings.TrimSpace(message),
	}
}

var _ = ctl.CodeSuccess

func jobStateView(job rtcontrol.JobState) map[string]interface{} {
	lastAt := job.UpdatedAt
	if job.FinishedAt > 0 {
		lastAt = job.FinishedAt
	}
	durationMs := int64(0)
	if job.CreatedAt > 0 && lastAt >= job.CreatedAt {
		durationMs = lastAt - job.CreatedAt
	}
	return map[string]interface{}{
		"trace_id":     job.TraceID,
		"device_code":  job.DeviceCode,
		"product_code": job.ProductCode,
		"kind":         job.Kind,
		"root_kind":    jobRootKind(job.Kind),
		"identifier":   job.Identifier,
		"code":         job.Code,
		"message":      job.Message,
		"created_at":   job.CreatedAt,
		"updated_at":   job.UpdatedAt,
		"finished_at":  job.FinishedAt,
		"duration_ms":  durationMs,
		"final":        rtcontrol.IsFinalCode(job.Code),
		"status_phase": jobStatusPhase(job.Code),
	}
}

func controlJobQueryView(query httpserver.ControlJobListQuery, limit int, offset int) map[string]interface{} {
	return map[string]interface{}{
		"device_code":  strings.TrimSpace(query.DeviceCode),
		"kind":         strings.TrimSpace(query.Kind),
		"identifier":   strings.TrimSpace(query.Identifier),
		"final":        query.Final,
		"final_set":    query.FinalSet,
		"created_from": query.CreatedFrom,
		"created_to":   query.CreatedTo,
		"updated_from": query.UpdatedFrom,
		"updated_to":   query.UpdatedTo,
		"limit":        limit,
		"offset":       offset,
	}
}

func normalizedJobLimit(limit int) int {
	if limit <= 0 {
		return 20
	}
	if limit > 200 {
		return 200
	}
	return limit
}

func normalizedExportLimit(limit int) int {
	if limit <= 0 {
		return 500
	}
	if limit > 5000 {
		return 5000
	}
	return limit
}

func normalizedEventLimit(limit int) int {
	if limit <= 0 {
		return 20
	}
	if limit > 200 {
		return 200
	}
	return limit
}

func normalizedJobOffset(offset int) int {
	if offset < 0 {
		return 0
	}
	return offset
}

func jobStatusPhase(code int) string {
	switch code {
	case ctl.CodeProcessing:
		return "processing"
	case ctl.CodeAccepted:
		return "accepted"
	case ctl.CodeSuccess:
		return "success"
	case ctl.CodePartialSuccess:
		return "partial_success"
	default:
		if rtcontrol.IsFinalCode(code) {
			return "failed"
		}
		return "pending"
	}
}

func jobRootKind(kind string) string {
	kind = strings.TrimSpace(kind)
	if idx := strings.Index(kind, ":"); idx >= 0 {
		return kind[:idx]
	}
	return kind
}

type diagnosticsBucket struct {
	Name            string
	Count           int
	LatestUpdatedAt int64
}

func controlJobDiagnosticsBreakdown(items []rtcontrol.JobState) map[string]interface{} {
	deviceBuckets := make(map[string]*diagnosticsBucket)
	identifierBuckets := make(map[string]*diagnosticsBucket)
	rootKindBuckets := make(map[string]*diagnosticsBucket)
	statusBuckets := make(map[string]*diagnosticsBucket)
	for _, item := range items {
		incrementBucket(deviceBuckets, strings.TrimSpace(item.DeviceCode), item.UpdatedAt)
		incrementBucket(identifierBuckets, strings.TrimSpace(item.Identifier), item.UpdatedAt)
		incrementBucket(rootKindBuckets, jobRootKind(item.Kind), item.UpdatedAt)
		incrementBucket(statusBuckets, jobStatusPhase(item.Code), item.UpdatedAt)
	}
	return map[string]interface{}{
		"devices":       diagnosticsBucketViews(deviceBuckets, "device_code"),
		"identifiers":   diagnosticsBucketViews(identifierBuckets, "identifier"),
		"root_kinds":    diagnosticsBucketViews(rootKindBuckets, "root_kind"),
		"status_phases": diagnosticsBucketViews(statusBuckets, "status_phase"),
	}
}

func incrementBucket(items map[string]*diagnosticsBucket, name string, updatedAt int64) {
	name = strings.TrimSpace(name)
	if name == "" {
		name = "-"
	}
	bucket, ok := items[name]
	if !ok {
		bucket = &diagnosticsBucket{Name: name}
		items[name] = bucket
	}
	bucket.Count++
	if updatedAt > bucket.LatestUpdatedAt {
		bucket.LatestUpdatedAt = updatedAt
	}
}

func diagnosticsBucketViews(items map[string]*diagnosticsBucket, key string) []map[string]interface{} {
	buckets := make([]diagnosticsBucket, 0, len(items))
	for _, item := range items {
		buckets = append(buckets, *item)
	}
	sort.Slice(buckets, func(i, j int) bool {
		if buckets[i].Count != buckets[j].Count {
			return buckets[i].Count > buckets[j].Count
		}
		if buckets[i].LatestUpdatedAt != buckets[j].LatestUpdatedAt {
			return buckets[i].LatestUpdatedAt > buckets[j].LatestUpdatedAt
		}
		return buckets[i].Name < buckets[j].Name
	})
	views := make([]map[string]interface{}, 0, len(buckets))
	for _, bucket := range buckets {
		views = append(views, map[string]interface{}{
			key:                 bucket.Name,
			"count":             bucket.Count,
			"latest_updated_at": bucket.LatestUpdatedAt,
		})
	}
	return views
}

func exportFilenameExt(format string) string {
	if strings.TrimSpace(format) == "csv" {
		return "csv"
	}
	return "json"
}

func controlResultEventView(sequence int, result ctl.Result) map[string]interface{} {
	return map[string]interface{}{
		"sequence":     sequence,
		"trace_id":     result.TraceID,
		"code":         result.Code,
		"message":      result.Message,
		"time":         result.Time,
		"data":         ensureDataMap(result.Data),
		"final":        rtcontrol.IsFinalCode(result.Code),
		"status_phase": jobStatusPhase(result.Code),
	}
}

func ensureDataMap(data map[string]interface{}) map[string]interface{} {
	if data == nil {
		return map[string]interface{}{}
	}
	return data
}
