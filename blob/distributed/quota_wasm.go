/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

//go:build js && wasm

package distributed

import "syscall/js"

// wasmSentinelTotal is the "unlimited" value reported when the browser
// declines to provide a storage estimate. 4 GiB is generous for
// origin-private file systems but far below the roadmap 50 MB max-file
// ceiling, so it cannot accidentally exempt tabs from quota control.
const wasmSentinelTotal int64 = 4 * 1024 * 1024 * 1024

// defaultStatter in the browser queries navigator.storage.estimate().
// The promise is awaited synchronously via a channel; if the API is
// unavailable the large-sentinel fallback keeps writes flowing.
type defaultStatter struct{}

func (defaultStatter) DiskStats(path string) (int64, int64, error) {
	_ = path // storage is per-origin in the browser

	navigator := js.Global().Get("navigator")
	if !navigator.Truthy() {
		return wasmSentinelTotal, wasmSentinelTotal, nil
	}
	storage := navigator.Get("storage")
	if !storage.Truthy() {
		return wasmSentinelTotal, wasmSentinelTotal, nil
	}
	estimate := storage.Get("estimate")
	if estimate.Type() != js.TypeFunction {
		return wasmSentinelTotal, wasmSentinelTotal, nil
	}

	type result struct {
		total, free int64
	}
	done := make(chan result, 1)

	var thenFn, catchFn js.Func
	thenFn = js.FuncOf(func(this js.Value, args []js.Value) interface{} {
		defer thenFn.Release()
		if len(args) == 0 {
			done <- result{wasmSentinelTotal, wasmSentinelTotal}
			return nil
		}
		obj := args[0]
		var total, usage int64
		if v := obj.Get("quota"); v.Truthy() {
			total = int64(v.Float())
		}
		if v := obj.Get("usage"); v.Truthy() {
			usage = int64(v.Float())
		}
		if total == 0 {
			done <- result{wasmSentinelTotal, wasmSentinelTotal}
			return nil
		}
		free := total - usage
		if free < 0 {
			free = 0
		}
		done <- result{total, free}
		return nil
	})
	catchFn = js.FuncOf(func(this js.Value, args []js.Value) interface{} {
		defer catchFn.Release()
		done <- result{wasmSentinelTotal, wasmSentinelTotal}
		return nil
	})
	storage.Call("estimate").Call("then", thenFn).Call("catch", catchFn)

	r := <-done
	return r.total, r.free, nil
}
