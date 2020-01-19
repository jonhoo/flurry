<!--
[![Crates.io](https://img.shields.io/crates/v/flurry.svg)](https://crates.io/crates/flurry)
[![Documentation](https://docs.rs/flurry/badge.svg)](https://docs.rs/flurry/)
-->
[![Build Status](https://dev.azure.com/jonhoo/jonhoo/_apis/build/status/flurry?branchName=master)](https://dev.azure.com/jonhoo/jonhoo/_build/latest?definitionId=15&branchName=master)
[![Codecov](https://codecov.io/github/jonhoo/flurry/coverage.svg?branch=master)](https://codecov.io/gh/jonhoo/flurry)
[![Dependency status](https://deps.rs/repo/github/jonhoo/flurry/status.svg)](https://deps.rs/repo/github/jonhoo/flurry)

A port of Java's `java.util.concurrent.ConcurrentHashMap` to Rust.

The port is based on the GPL-licensed [source file from JDK13] as of
commit [`0368f3a073a9`], and is also GPL-licensed. The source file is
also included in this repository for easy reference. Eventually we will
probably also port the [upstream tests].

It is under active development as a part of a series of [live coding
streams], kicked off by [this tweet].

  [source file from JDK13]: https://hg.openjdk.java.net/jdk/jdk13/file/tip/src/java.base/share/classes/java/util/concurrent/ConcurrentHashMap.java
  [`0368f3a073a9`]: https://hg.openjdk.java.net/jdk/jdk13/file/0368f3a073a9/src/java.base/share/classes/java/util/concurrent/ConcurrentHashMap.java
  [live coding streams]: https://www.youtube.com/playlist?list=PLqbS7AVVErFj824-6QgnK_Za1187rNfnl
  [this tweet]: https://twitter.com/jonhoo/status/1194969578855714816
  [upstream tests]: https://hg.openjdk.java.net/jdk/jdk13/file/tip/test/jdk/java/util/concurrent/ConcurrentHashMap
