# Changelog

## [0.1.0](https://github.com/bbmumford/ledger/compare/v0.0.21...v0.1.0) (2026-11-26)


### ⚠ BREAKING CHANGES

* github.com/bbmumford/ledger/blob/distributed and blob/distributed/onchain are removed; import github.com/bbmumford/diststore/onchain instead.
* the JSON form of a record names the author key authorPubKey, and LADRecord's Go field is AuthorPubKey.
* Append and BatchAppend refuse a record that carries a signature and no Seq (ErrSignedWithoutSeq); sign such a record through AppendSigned or give it its Seq first.
* LADRecord's partition is named partitionId

### Features

* a record's author key is authorPubKey in the proto, the column and the JSON form ([daea600](https://github.com/bbmumford/ledger/commit/daea600a9bca8959fa31bd948c010b8f317eae1d))
* ledger no longer carries the blob/distributed packages ([45d9fdc](https://github.com/bbmumford/ledger/commit/45d9fdcd7824475ba02b3238058cd51b5a6a0645))


### Bug Fixes

* a ledger numbers a record before it is signed, inside the transaction that stores it ([6d11d1e](https://github.com/bbmumford/ledger/commit/6d11d1e5961c55c72be4d60ddf1195c766e54a21))
* go.mod requires whisper v0.0.20, the whisper release this module builds and tests against ([6a70d35](https://github.com/bbmumford/ledger/commit/6a70d35f02b731f161341b8999172ed09cebc849))
* LocalDBLedger stores every field of a record, so a replayed record keeps its author key and tombstone and still verifies ([a314ec3](https://github.com/bbmumford/ledger/commit/a314ec38f389b78fd04cf11e410d6c77521e14ef))


### Code Refactoring

* LADRecord's partition is named partitionId ([26d1764](https://github.com/bbmumford/ledger/commit/26d17648b9834abdb4a89fd6609b2ce0b18e7f3e))

## [0.0.21](https://github.com/bbmumford/ledger/compare/v0.0.20...v0.0.21) (2026-11-15)


### Bug Fixes

* build against published whisper v0.0.19 and aether ([acbd1f1](https://github.com/bbmumford/ledger/commit/acbd1f18143a15d0684861700a72c91fc98e6a0c))
* check every returned error and mark the unused code kept on purpose, so the release lint passes ([bf288ba](https://github.com/bbmumford/ledger/commit/bf288ba62cd58173594e7c77beba50791642a8a2))
