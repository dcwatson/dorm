#!/usr/bin/env python
# -*- coding: utf-8 -*-

import unittest
import darby
import sqlite3
import shutil
import os
import sys


class Book(darby.Table):
    columns = {"name": darby.String, "year": darby.Integer}


class CustomKey(darby.Table):
    columns = {"key": darby.PK, "label": darby.String}


class TableTests(unittest.TestCase):
    def setUp(self):
        darby.setup(models=[Book, CustomKey])

    def test_lifecycle(self):
        book = Book.insert(name="First Book", year=2019)
        book.year = 2020
        book.save().refresh()
        self.assertEqual(book.year, 2020)

    def test_get(self):
        self.assertIsNone(Book.query(pk=1).get())
        with self.assertRaises(darby.DoesNotExist):
            Book.query(pk=1).get(strict=True)
        Book.insert(name="First Book", year=2019)
        self.assertEqual(Book.query(name="First Book").get("year"), 2019)
        Book.insert(name="Second Book", year=2019)
        with self.assertRaises(darby.MultipleObjects):
            Book.query(year=2019).get(strict=True)

    def test_insert_pk(self):
        book = Book.insert(pk=999, name="Some Book", year=2019).refresh()
        self.assertEqual(book.pk, 999)
        with self.assertRaises(sqlite3.IntegrityError):
            Book.insert(pk=999, name="Another Book", year=2019)

    def test_order(self):
        Book.insert(name="1 Bourbon", year=2020)
        Book.insert(name="1 Scotch", year=2020)
        Book.insert(name="1 Beer", year=2021)
        self.assertEqual(
            list(Book.query().order("-year", "name").values_list("name", flat=True)),
            ["1 Beer", "1 Bourbon", "1 Scotch"],
        )
        self.assertEqual(
            list(Book.query(year=2020).order("-name").values("name")),
            [{"name": "1 Scotch"}, {"name": "1 Bourbon"}],
        )

    def test_custom_pk(self):
        obj = CustomKey.insert(pk=13, label="Lucky 13")
        self.assertEqual(obj.pk, obj.key)


class MigrationTests(unittest.TestCase):
    def setUp(self):
        self.db_path = "test.db"
        self.migration_dir = os.path.join(os.path.dirname(__file__), "test_migrations")
        os.makedirs(self.migration_dir, exist_ok=True)
        with open(os.path.join(self.migration_dir, "__init__.py"), "w") as f:
            f.write("\n")

    def tearDown(self):
        shutil.rmtree(self.migration_dir)
        os.remove(self.db_path)
        del sys.modules["test_migrations"]

    def test_generate(self):
        # Generate the migrations (but they aren't run until next setup).
        darby.setup(
            self.db_path, models=[Book], migrations="test_migrations", generate=True
        )
        self.assertFalse(darby.Migration.exists())
        self.assertFalse(Book.exists())
        # This will run any previously created migrations.
        darby.setup(self.db_path, models=[Book], migrations="test_migrations")
        self.assertTrue(darby.Migration.exists())
        self.assertTrue(Book.exists())
        Book.insert(name="Test Book", year=2019)
        # Add a new column, queries will fail until a new migration is generated/applied.
        Book.columns["author"] = darby.String
        with self.assertRaises(sqlite3.OperationalError):
            Book.query().get()
        # Generate a migration for the new column.
        darby.setup(
            self.db_path, models=[Book], migrations="test_migrations", generate=True
        )
        # Run the migration.
        darby.setup(self.db_path, models=[Book], migrations="test_migrations")
        Book.update({"year": 2019}, author="Dan Watson")
        self.assertEqual(Book.query(year=2019).get("author"), "Dan Watson")
        del Book.columns["author"]


if __name__ == "__main__":
    unittest.main()