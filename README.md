Super Light FTP Server
===

Author: [Gerald](http://gerald.top) \<gera2ld@163.com\>

Features
---
* Multi-user
* Transport speed limit
* Config files

Requirements
---
* Python 3.4+ (`asyncio` module is required)

Installation
---
``` sh
$ python3 setup.py install
```
or just copy `./slftpd`.

Usage
---
Config files are in the package directory (global settings) or `~/.gerald/ftpd.cfg` (user settings).

``` sh
$ python3 -m slftpd
```
