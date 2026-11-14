module github.com/bbmumford/ledger

go 1.25.0

require (
	github.com/bbmumford/whisper v0.0.15
	golang.org/x/crypto v0.50.0
	google.golang.org/protobuf v1.36.11
)

require (
	github.com/klauspost/cpuid/v2 v2.3.0 // indirect
	github.com/klauspost/reedsolomon v1.13.3 // indirect
	golang.org/x/sys v0.43.0 // indirect
	golang.org/x/time v0.15.0 // indirect
)

replace github.com/ORBTR/aether => ../aether

replace github.com/bbmumford/whisper => ../whisper
