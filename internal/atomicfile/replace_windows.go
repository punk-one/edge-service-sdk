//go:build windows

package atomicfile

import (
	"os"

	"golang.org/x/sys/windows"
)

func replaceFile(temporary, target string) error {
	from, err := windows.UTF16PtrFromString(temporary)
	if err != nil {
		return err
	}
	to, err := windows.UTF16PtrFromString(target)
	if err != nil {
		return err
	}
	return windows.MoveFileEx(from, to, windows.MOVEFILE_REPLACE_EXISTING|windows.MOVEFILE_WRITE_THROUGH)
}

func replaceFileIfAbsent(temporary, target string) (bool, error) {
	from, err := windows.UTF16PtrFromString(temporary)
	if err != nil {
		return false, err
	}
	to, err := windows.UTF16PtrFromString(target)
	if err != nil {
		return false, err
	}
	if err := windows.MoveFileEx(from, to, windows.MOVEFILE_WRITE_THROUGH); err != nil {
		if os.IsExist(err) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}
