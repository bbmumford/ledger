/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

//go:build (linux || darwin || freebsd || netbsd || openbsd || dragonfly) && !js

package distributed

import (
	"fmt"

	"golang.org/x/sys/unix"
)

func platformDiskStats(path string) (int64, int64, error) {
	if path == "" {
		path = "."
	}
	var st unix.Statfs_t
	if err := unix.Statfs(path, &st); err != nil {
		return 0, 0, fmt.Errorf("distributed: statfs %q: %w", path, err)
	}
	total := int64(st.Blocks) * int64(st.Bsize)
	free := int64(st.Bavail) * int64(st.Bsize)
	return total, free, nil
}
