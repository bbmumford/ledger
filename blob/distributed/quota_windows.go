/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

//go:build windows

package distributed

import (
	"fmt"

	"golang.org/x/sys/windows"
)

func platformDiskStats(path string) (int64, int64, error) {
	if path == "" {
		path = "."
	}
	p, err := windows.UTF16PtrFromString(path)
	if err != nil {
		return 0, 0, fmt.Errorf("distributed: utf16 %q: %w", path, err)
	}
	var freeAvail, total, free uint64
	if err := windows.GetDiskFreeSpaceEx(p, &freeAvail, &total, &free); err != nil {
		return 0, 0, fmt.Errorf("distributed: GetDiskFreeSpaceEx %q: %w", path, err)
	}
	return int64(total), int64(freeAvail), nil
}
