from setuptools import setup

import dorm


setup(
    name="dorm",
    version=dorm.version,
    description="A tiny SQLite ORM for Python.",
    author="Dan Watson",
    author_email="dcwatson@gmail.com",
    url="https://github.com/dcwatson/dorm",
    license="MIT",
    py_modules=["dorm"],
    classifiers=[
        "Development Status :: 2 - Pre-Alpha",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3",
        "Topic :: Database",
    ],
)
