# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]
### Added

### Changed

### Removed

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

## [0.1.0] - 2020-02-04
### Added
- First "real" release.

[Unreleased]: https://github.com/jonhoo/inferno/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/jonhoo/inferno/compare/6cda7c8e7501b9b7453e7a2db269a994a99c0660...v0.1.0
