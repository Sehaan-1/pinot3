# Contributing

This project is built **slice by slice, one PR per slice**. Every PR must keep
`main` green, compilable, and testable. The full plan lives in
[docs/ROADMAP.md](docs/ROADMAP.md).

## Ground rules

1. **One slice = one PR.** A PR has a single responsibility, a bounded diff,
   and one clear verification command. Never bundle multiple layers or
   unrelated connectors.
2. **Strict dependency order.** Never implement a downstream module before its
   upstream contracts are merged and tested. In particular:
   `seatunnel-api` must never depend on `seatunnel-engine` or
   `seatunnel-connectors-v2`.
3. **Reuse first.** Before writing any utility, parser, scheduler, or protocol
   from scratch, check whether a proven Maven dependency solves it. Default to
   adopting the library; write only the thin adapter. (See the adopt-vs-build
   matrix in the roadmap.)
4. **No silent stubs.** No dummy `TODO`s or mocked-out core paths. If something
   is deferred, it is explicitly listed as out of scope in the PR description.

## Local workflow

Prerequisite: JDK 17. The Maven wrapper handles the rest.

```bash
# Full verification (exactly what CI runs)
./mvnw -B -ntp verify

# Format before committing
./mvnw spotless:apply

# Affected module + its dependencies only
./mvnw -pl <module> -am test

# Integration tests only (Failsafe, *IT classes)
./mvnw -pl <module> -am verify -DskipUnitTests
```

## PR checklist

- [ ] Single slice, single responsibility; out-of-scope items listed in the PR body.
- [ ] `./mvnw spotless:apply` run; `./mvnw -B -ntp verify` green locally.
- [ ] New public contracts have unit tests (and, from Slice 5, an E2E job test).
- [ ] New libraries are pinned in the root POM's `dependencyManagement` — no
      version strings scattered in child POMs.
- [ ] No circular dependencies; layer boundaries respected (check with
      `./mvnw dependency:tree` if unsure).
- [ ] PR description states: changes, libraries adopted, contracts locked,
      test commands, and the next slice's scope.

## Code style

- Java 17, google-java-format via Spotless (`./mvnw spotless:apply`).
- Apache-2.0 license header on every Java file (enforced by
  `license-maven-plugin`; run `./mvnw license:format` if a header is missing).
- Prefer Guava / Apache Commons over hand-rolled helpers.
