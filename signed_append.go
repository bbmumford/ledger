/*
 * Copyright (c) 2026 ORBTR Pty Ltd. All Rights Reserved.
 */

package ledger

import (
	"context"
	"errors"
)

// ErrSignedWithoutSeq refuses a record handed to Append or BatchAppend with a
// signature and no Seq. The ledger would give it a Seq after the signature was
// made, and Seq is part of the signed bytes (signatureContent), so the stored
// record would never verify. A caller that signs a record the ledger numbers
// appends it unsigned through SigningLedger, whose sign function runs after the
// number is given.
var ErrSignedWithoutSeq = errors.New("ledger: a signed record has no Seq; the ledger would number it after its signature")

// SignFunc signs a record the ledger has numbered: Seq and, when it was zero,
// Timestamp are set before it runs. It sets the record's AuthorPubKey and
// Signature (SignRecord), or leaves a record it does not sign as it is. An
// error stores nothing.
type SignFunc func(rec *Record) error

// SigningLedger is a Ledger that numbers a record before it is signed.
// AppendSigned gives rec its Seq (when 0) and Timestamp (when zero), calls sign
// on the numbered record, and stores what sign left, all in the one step that
// no other append on the ledger interleaves with, so the signature covers the
// Seq the ledger stored. BatchAppendSigned does the same for every record of a
// batch, and stores all of them or none.
type SigningLedger interface {
	Ledger
	AppendSigned(ctx context.Context, rec Record, sign SignFunc) error
	BatchAppendSigned(ctx context.Context, records []Record, sign SignFunc) error
}
