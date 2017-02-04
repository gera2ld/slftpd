slftpd
===

Super light FTP server, written in pure Python.

Requirements: Python 3.5+ (`asyncio` module is required)

Features
---
* Multi-user
* Transport speed limit
* Zero config

Installation
---
``` sh
$ pip3 install git+https://github.com/gera2ld/slftpd.git
```

Usage
---
Command line usage:
``` sh
$ python3 -m slftpd -p 8021 -H ~
```
