package control

const (
	CodeSuccess        = 0
	CodeProcessing     = 102
	CodeAccepted       = 202
	CodePartialSuccess = 206
	CodeBadRequest     = 400
	// CodeAmbiguous means execution may have reached the physical device but
	// the SDK could not durably observe the outcome. It must not be auto-retried.
	CodeAmbiguous    = 409
	CodeNotFound     = 404
	CodeNotSupported = 405
	CodeExpired      = 410
	CodeDriverError  = 501
	CodeBusy         = 503
	CodeTimeout      = 504
)
