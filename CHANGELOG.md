# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]
### Added

### Changed

### Removed

## [0.5.2] - 2024-11-19
### Added

- Alternative (better) crates section ([#128])

[#128]: https://github.com/jonhoo/flurry/pull/128

## [0.5.1] - 2024-04-21
### Changed
- Updated to `seize 0.3` ([#123])
- Fixed long-standing miri warning ([#123])

## [0.5.0] - 2024-02-11
### Changed
- (BREAKING) Updated to `ahash 0.8` ([#121])

## [0.4.0] - 2022-02-26
### Changed
- Moved memory management to [`seize`]() ([#102])
- Bumped `ahash` and `parking_lot` ([#105])

[`seize`]: https://docs.rs/seize/latest/seize/
[#102]: https://github.com/jonhoo/flurry/pull/102
[#105]: https://github.com/jonhoo/flurry/pull/105

## [0.3.1] - 2020-08-28
### Added
- Basic `rayon` support (#89)
- Miri leak checking

### Changed
- Fixed panic when `no_replacement` is used

## [0.3.0] - 2020-04-13
### Added
- `HashMap::try_insert` (#74)
- `HashSetRef` (#78)
- Serialization support with `serde` (#79; behind a feature flag).

### Changed
- Changelog. Which will now (in theory) be updated with every release.
- We now implement Java's "tree bin optimization" (#72).
- Many more tests have been ported over.
- Fixed several memory leaks.

## 0.1.0 - 2020-02-04
### Added
- First "real" release.

[Unreleased]: https://github.com/jonhoo/flurry/compare/v0.5.2...HEAD
[0.5.2]: https://github.com/jonhoo/flurry/compare/v0.5.1...v0.5.2
[0.5.1]: https://github.com/jonhoo/flurry/compare/v0.5.0...v0.5.1
[0.5.0]: https://github.com/jonhoo/flurry/compare/v0.4.0...v0.5.0
[0.4.1]: https://github.com/jonhoo/flurry/compare/v0.4.0...v0.5.0
[0.4.0]: https://github.com/jonhoo/flurry/compare/v0.3.1...v0.4.0
[0.3.1]: https://github.com/jonhoo/flurry/compare/v0.3.0...v0.3.1
[0.3.0]: https://github.com/jonhoo/flurry/compare/v0.2.1...v0.3.0
