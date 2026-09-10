//go:build windows

package configfile

// PermissionsTooOpen is disabled on Windows because Unix mode bits do not
// represent the effective ACL protecting a file.
func PermissionsTooOpen(string) (bool, error) {
	return false, nil
}
