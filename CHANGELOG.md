# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- Sender address validation at startup: `notification.mail.from` must be non-blank and a valid e-mail address;
  invalid configuration prevents `notification-service` from starting (#23, @demonipun)
- Structured JSON logs with shared field names, `X-Correlation-Id` on every request, and consumed / skipped /
  failed events logged with their ids in `loyalty-service` (#25, @dmitriy-iliyov)

### Changed

- Bumped the JaCoCo plugin from 0.8.14 to 0.8.16 (#19, @ahin042)
- Bumped Spring Boot from 3.5.14 to 3.5.16 (#18, @ahin042)

## [0.1.0] - 2026-11-14

### Added

- `order-service`: orders API (create, get, cancel) with RFC 9457 errors and OpenAPI documentation
- `order-service`: order events published through a transactional outbox, with a `published_events` log
- `loyalty-service`: loyalty points accrued and reverted through an effectively-once inbox, points API
- `notification-service`: order confirmation e-mails through an at-most-once inbox, notifications API
- Dead letter topic `orders.events.DLT` for events that fail every retry
- Docker Compose infrastructure for running the services from source

[Unreleased]: https://github.com/dmitriy-iliyov/spring-transactional-inbox/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/dmitriy-iliyov/spring-transactional-inbox/releases/tag/v0.1.0
