# archive_stats

Tools and scripts for MWA Archive stats collection

NOTE: this requires direct database access to the MWA ASVO job database, MWA metadata database, the MWA VO TAP service and, optionally, the Pawsey Acacia and Banksia endpoints.

## installation

Using uv:

```bash
$ uv sync --upgrade
$ source .venv/bin/activate
(archive_stats) $
```

## usage

```bash
usage: 

(archive_stats) $ python main.py [-h] -c CFG [-n]

archive_stats calculates usage from info dervied from the MWA database, TAP service, Acacia and Banksia. Needs to be run from a machine with access to Acacia/Banksia and the MWA database.

options:
  -h, --help       show this help message and exit
  -c, --cfg CFG    Configuration file location.
  -n, --no-pawsey  Do not get Pawsey data sizes.
```