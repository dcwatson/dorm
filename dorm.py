import argparse
import datetime
import importlib
import inspect
import itertools
import logging
import os
import pkgutil
import re
import sqlite3


version_info = (0, 1, 0)
version = ".".join(str(v) for v in version_info)

logger = logging.getLogger(__name__)


class DatabaseError(Exception):
    pass


class DoesNotExist(DatabaseError):
    pass


class MultipleObjects(DatabaseError):
    pass


def snake(name):
    s1 = re.sub("(.)([A-Z][a-z]+)", r"\1_\2", name)
    return re.sub("([a-z0-9])([A-Z])", r"\1_\2", s1).lower()


class Column:
    def __init__(
        self,
        sql_type,
        notnull=False,
        primary_key=False,
        default=None,
        to_python=None,
        to_sql=None,
    ):
        self.sql_type = sql_type
        self.notnull = notnull
        self.primary_key = primary_key
        self.default = default
        self.to_python = to_python or (lambda value: value)
        self.to_sql = to_sql or (lambda value: value)

    def typedef(self, name):
        sql = "{} {}".format(name, self.sql_type)
        if self.notnull:
            sql += " NOT NULL"
        if self.primary_key:
            if not self.sql_type.lower().startswith("int"):
                raise DatabaseError(
                    "Only integer PRIMARY KEYs are supported currently."
                )
            sql += " PRIMARY KEY"
        if self.default is not None:
            d = self.default() if callable(self.default) else self.default
            sql += " DEFAULT {}".format(d)
        return sql


PK = Column("integer", primary_key=True)
String = Column("varchar", notnull=True, default="''")
Integer = Column("integer")
Timestamp = Column("timestamp", notnull=True, default="CURRENT_TIMESTAMP")


class Query:
    def __init__(self, table):
        self.table = table
        self._filters = {}
        self._order = []
        self._limit = None

    def copy(self, filters=None, order=None, limit=None, fields=None):
        other = self.__class__(self.table)
        other._filters = self._filters.copy()
        if filters:
            other._filters.update(filters)
        other._order = order if order is not None else self._order[:]
        other._limit = limit if limit is not None else self._limit
        return other

    def filter(self, **kwargs):
        pk = kwargs.pop("pk", None)
        if pk is not None:
            kwargs[self.table.__pk__] = pk
        return self.copy(filters=kwargs)

    def order(self, *fields):
        return self.copy(order=fields)

    def limit(self, limit):
        return self.copy(limit=limit)

    def get(self, field=None, default=None, strict=False):
        objects = list(self.limit(2 if strict else 1))
        if strict and not objects:
            raise DoesNotExist(
                "Query returned no {} objects.".format(self.table.__name__)
            )
        if strict and len(objects) > 1:
            raise MultipleObjects(
                "Query returned multiple {} objects.".format(self.table.__name__)
            )
        first = objects[0] if objects else default
        return first if field is None else getattr(first, field, default)

    def to_sql(self, selects=None):
        if selects is None:
            selects = list(self.table.columns.keys())
            if self.table.__pk__ not in selects:
                selects.insert(0, self.table.__pk__)
        sql = "SELECT {} FROM {}".format(", ".join(selects), self.table.__table__)
        where = []
        params = []
        for field, value in self._filters.items():
            where.append("{} = ?".format(field))
            params.append(value)
        if where:
            sql += " WHERE {}".format(" AND ".join(where))
        orders = []
        for field in self._order:
            desc = field.startswith("-")
            field = field.lstrip("-")
            if field in self.table.columns:
                orders.append("{} {}".format(field, "DESC" if desc else "ASC"))
        if orders:
            sql += " ORDER BY {}".format(", ".join(orders))
        if self._limit:
            sql += " LIMIT {}".format(self._limit)
        return sql, params

    def values(self, *fields):
        if not fields:
            fields = list(self.table.columns.keys())
        sql, params = self.to_sql(fields)
        for row in self.table.raw(sql, params):
            yield {f: row[f] for f in fields}

    def values_list(self, *fields, **kwargs):
        if not fields:
            fields = list(self.table.columns.keys())
        sql, params = self.to_sql(fields)
        for row in self.table.raw(sql, params):
            if kwargs.get("flat"):
                for f in fields:
                    yield row[f]
            else:
                yield [row[f] for f in fields]

    def count(self):
        sql, params = self.to_sql(selects=["count(*)"])
        return self.table.raw(sql, params).fetchone()[0]

    def __iter__(self):
        sql, params = self.to_sql()
        for row in self.table.raw(sql, params):
            fields = {}
            for key in row.keys():
                if key in self.table.columns:
                    fields[key] = self.table.columns[key].to_python(row[key])
                else:
                    fields[key] = row[key]
            yield self.table(**fields)


class Table:
    __table__ = None
    __connection__ = None
    __pk__ = "rowid"

    columns = {}

    def __init__(self, **fields):
        pk = fields.pop("pk", None)
        if pk is not None:
            fields[self.__class__.__pk__] = pk
        self.__dict__.update(fields)

    def __repr__(self):
        return "<{} pk={}>".format(self.__class__.__name__, self.pk)

    def get_pk(self):
        return getattr(self, self.__class__.__pk__, None)

    def set_pk(self, value):
        setattr(self, self.__class__.__pk__, value)

    pk = property(get_pk, set_pk)

    def save(self, force_insert=False):
        # Lists of fields and values to insert/update, excluding PK.
        names = []
        params = []
        for name, col in self.__class__.columns.items():
            if not col.primary_key and hasattr(self, name):
                names.append(name)
                params.append(col.to_sql(getattr(self, name)))
        # TODO: upserts would make this a little cleaner/safer, but they aren't available until 3.24.0.
        if force_insert or not self.pk:
            if self.pk:
                names.insert(0, self.__class__.__pk__)
                params.insert(0, self.pk)
            sql = "INSERT INTO {} ({}) VALUES ({})".format(
                self.__class__.__table__,
                ", ".join(names),
                ", ".join("?" for n in names),
            )
            self.pk = self.__class__.raw(sql, params).lastrowid
        else:
            self.__class__.update(self, **dict(zip(names, params)))
        return self

    def refresh(self):
        obj = self.__class__.query(pk=self.pk).get()
        self.__dict__.update(obj.__dict__)
        return self

    @classmethod
    def bind(cls, connection, inspect=False):
        if isinstance(inspect, str):
            cls.__table__ = inspect
        if not cls.__table__:
            cls.__table__ = snake(cls.__name__)
        cls.__connection__ = connection
        if inspect:
            for row in cls.raw("pragma table_info({})".format(cls.__table__)):
                cls.columns[row["name"]] = Column(
                    row["type"],
                    notnull=row["notnull"],
                    primary_key=row["pk"],
                    default=row["dflt_value"],
                )
        for name, col in cls.columns.items():
            if col.primary_key:
                cls.__pk__ = name
        return cls

    @classmethod
    def exists(cls):
        for row in cls.raw(
            "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?",
            (cls.__table__,),
        ):
            return True
        return False

    @classmethod
    def raw(cls, sql, params=None):
        logger.debug("%s :: %s %s", cls.__name__, sql, params or [])
        return cls.__connection__.execute(sql, params or [])

    @classmethod
    def schema_changes(cls):
        table_name = cls.__table__
        current = {}
        for row in cls.raw("pragma table_info({})".format(table_name)).fetchall():
            current[row["name"]] = row["type"]
        if current:
            for name, col in cls.columns.items():
                if name in current:
                    # Check for type mismatches and warn the user
                    col_type = col.sql_type.split()[0]
                    if current[name] != col_type:
                        logger.warning(
                            "Type mismatch for {}.{} ({} vs. {})".format(
                                table_name, name, current[name], col_type
                            )
                        )
                else:
                    # Create nonexistent columns
                    yield "ALTER TABLE {} ADD COLUMN {}".format(
                        table_name, col.typedef(name)
                    )
            for name in current:
                if name not in cls.columns:
                    logger.warning("Orphaned column {}.{}".format(table_name, name))
        else:
            parts = [col.typedef(name) for name, col in cls.columns.items()]
            yield "CREATE TABLE {} ({})".format(table_name, ", ".join(parts))

    @classmethod
    def query(cls, **kwargs):
        return Query(cls).filter(**kwargs)

    @classmethod
    def insert(cls, **fields):
        return cls(**fields).save(force_insert=True)

    @classmethod
    def update(cls, where, **fields):
        updates = []
        params = []
        wheres = []
        for field, value in fields.items():
            if field in cls.columns:
                updates.append("{} = ?".format(field))
                params.append(value)
            else:
                logger.warning('Column "{}" does not exist'.format(field))
        if isinstance(where, cls):
            where = {cls.__pk__: where.pk}
        for field, value in where.items():
            if field in cls.columns or field == cls.__pk__:
                wheres.append("{} = ?".format(field))
                params.append(value)
        sql = "UPDATE {} SET {} WHERE {}".format(
            cls.__table__, ", ".join(updates), " AND ".join(wheres)
        )
        return cls.raw(sql, params).rowcount


class Migration(Table):
    columns = {"module": String, "name": String, "applied": Timestamp}

    @classmethod
    def write(cls, module, sql_statements):
        migration_dir = os.path.dirname(module.__file__)
        filename = datetime.datetime.utcnow().strftime("%Y%m%d_%H%M%S_%f.py")
        migration_path = os.path.join(migration_dir, filename)
        logger.info("Writing migration to {}".format(migration_path))
        with open(migration_path, "w") as f:
            f.write("def forward(connection):\n")
            if sql_statements:
                for sql in sql_statements:
                    f.write(
                        '    connection.execute("{}")\n'.format(sql.replace('"', '\\"'))
                    )
            else:
                f.write("    raise NotImplementedError()\n")

    @classmethod
    def migrate(cls, module, connection):
        latest = (
            cls.query(module=module.__name__).order("-applied").get("name")
            if cls.exists()
            else None
        )
        migration_names = {
            name
            for _, name, is_pkg in pkgutil.iter_modules(module.__path__)
            if not is_pkg and name[0] not in "_~"
        }
        for name in sorted(migration_names):
            # TODO: select all applied, and check for old skipped migrations which may indicate merges
            if latest is None or name > latest:
                mod = importlib.import_module("{}.{}".format(module.__name__, name))
                mod.forward(connection)
                cls.insert(module=module.__name__, name=name)


def setup(db_path=":memory:", models=None, migrations=None, generate=False, new=False):
    connection = sqlite3.connect(db_path)
    connection.isolation_level = None
    connection.row_factory = sqlite3.Row

    # Generate a list of Table classes to find schema changes for.
    tables = []
    if isinstance(models, str):
        mod = importlib.import_module(models)
        for name, cls in inspect.getmembers(mod, inspect.isclass):
            if issubclass(cls, Table) and cls not in tables:
                tables.append(cls.bind(connection))
    elif isinstance(models, (list, tuple)):
        for cls in models:
            if issubclass(cls, Table):
                tables.append(cls.bind(connection))

    # Migrate the database to either the latest migration (if using migrations), or the latest schema.
    if migrations:
        tables.append(Migration.bind(connection))
        mod = importlib.import_module(migrations)
        Migration.migrate(mod, connection)
        if generate:
            sql_statements = list(
                itertools.chain.from_iterable(t.schema_changes() for t in tables)
            )
            if sql_statements or new:
                Migration.write(mod, sql_statements)
    else:
        for sql in itertools.chain.from_iterable(t.schema_changes() for t in tables):
            connection.execute(sql)

    return connection


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", default=":memory:", help="The database file to use.")
    parser.add_argument(
        "--models",
        default=None,
        help="The models package to look for Table classes in.",
    )
    parser.add_argument(
        "--migrations", default=None, help="The package to store new migrations in."
    )
    parser.add_argument("command", choices=["migrate", "generate", "new"])
    args = parser.parse_args()
    generate = args.command in ("generate", "new")
    new = args.command == "new"
    setup(
        args.db,
        models=args.models,
        migrations=args.migrations,
        generate=generate,
        new=new,
    )
