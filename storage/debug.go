package storage

import ledger "github.com/bbmumford/ledger"

var dbgStorage = ledger.NewDebug("ledger.storage") //nolint:unused // the ledger.storage DEBUG subsystem's logger, declared beside ledger.storage.local for storage diagnostics
var dbgLocal = ledger.NewDebug("ledger.storage.local")
