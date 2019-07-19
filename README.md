# dorm

[![Build Status](https://travis-ci.org/dcwatson/dorm.svg?branch=master)](https://travis-ci.org/dcwatson/dorm)

A small SQLite ORM for Python 3. You probably shouldn't use this, but if you want to anyway, `pip install dorm`
and look at the [tests](https://github.com/dcwatson/dorm/blob/master/tests.py) for how to do so.


## Migrations

Dorm has the most basic migration support imaginable:

```
# Generates a schema migration if it detects any changes.
python -m dorm --db=books.db --models=project.models --migrations=project.migrations generate

# Migrates to the latest migration (sorted by filename in the --migrations module).
python -m dorm --db=books.db --models=project.models --migrations=project.migrations migrate
```

Existing migrations are run automatically when calling `dorm.setup` with the `migrations` argument set. This
is to ensure a good first-run experience and automatic upgrades for end users. If `migrations` is not set,
any detected schema changes will be applied automatically to the database.


## Asynchronous Tables

Dorm can be used with `asyncio` by simply subclassing `AsyncTable` instead of `Table`. The `insert` class method,
and `save` and `refresh` instance methods become coroutines in that case. Also, the `query` class method will return
an `AsyncQuery` instance, with coroutines for `count`, `values`, `get`, `update`, and iteration via `__aiter__`
(i.e. `async for obj in MyTable.query()`).

Migrations and introspection still happen synchronously, since they tend not to happen during times where they would
benefit from being asyncronous.
