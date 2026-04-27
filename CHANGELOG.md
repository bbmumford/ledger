# Changelog

## [0.0.2](https://github.com/bbmumford/ledger/compare/v0.0.1...v0.0.2) (2026-04-27)


### Features

* **cache:** expose GetLastReachBody for reach FreshnessClient lookup ([e0b4589](https://github.com/bbmumford/ledger/commit/e0b4589db32e5fefbae573a42fb2e63e75bda56b))
* **cache:** pluggable ReachDeltaApplier so deltas rebuild full records ([13ee31c](https://github.com/bbmumford/ledger/commit/13ee31c5e2cfc50e0be2ab2bbe4e68476f095cc0))
* ContentHash + IdentityView projection in DirectoryCache (v0.0.8) ([4bb73f9](https://github.com/bbmumford/ledger/commit/4bb73f9b9d4d1ed6ecfc87dbbdf3673090cff5e6))
* DirectoryCache self-exemption from liveness + cap eviction ([e78ad5d](https://github.com/bbmumford/ledger/commit/e78ad5deb807d370740571bc27a7d9455c2719e9))
* ReachRecord.Metadata + cache-derived MemberRecord views ([d294a32](https://github.com/bbmumford/ledger/commit/d294a32ef50d0ac7b64966ae0bf6f7523f0edb32))
* **whisper_adapter:** align CacheStateStore with new StateStore shape ([5a76a18](https://github.com/bbmumford/ledger/commit/5a76a188d9bc59bea381def42ff0df1ae2471982))


### Bug Fixes

* **cache:** skip reach-layer deltas so they don't clobber full snapshots ([17dfc4a](https://github.com/bbmumford/ledger/commit/17dfc4af47beb424bcce76d945146617806900f0))
