#!/usr/bin/env python3
# encoding=utf-8
from setuptools import setup


if __name__ == "__main__":
    setup(
        install_requires=[
            "appyratus",
            "starlette==0.17.1",
            "fastapi",
            "sse_starlette",
            "uvicorn[standard]",
            "pexpect",
        ]
    )
