# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to
[Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## 1.8.0 - 2023-11-09

### Changed

- Remove condition to skip `queryUntilLimitReached` when `filterExpression` is
  not provided

## 1.7.2- 2021-01-31

### Added

- The `multiIncr` API to support DynamoDB "ConditionalExpressions".

## 1.7.1- 2021-01-31

### Fixed

- Parameter type `IncrMap` of the `multiIncr` API to only be partial.

## 1.7.0 - 2021-01-31

### Added

- `multiIncr` API

## 1.4.0 - 2021-09-22

### Added

- Support `consistentRead` option on `get` API

## 1.5.0 - 2021-10-27

### Added

- Support optimistic locking for `put`, `update` and `delete` APIs
