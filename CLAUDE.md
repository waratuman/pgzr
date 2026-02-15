# CLAUDE.md

## Build & Test

```bash
zig build              # build library + examples
zig build test         # run unit tests
zig build lib          # build shared library (libpgzr.dylib/so)
```

## Pre-Commit Checklist

- Keep `docs/schema.md` in sync with `src/schema.zig`. Any change to the
  destination schema DDL must be reflected in the documentation before
  committing.
