# spanner-loader

## Installation

Download from https://github.com/tkuchiki/spanner-loader/releases

## Usage

```console
$ spanner-loader --help
Usage of ./spanner-loader:
  -c uint
        Concurrency (default 1)
  -d duration
        Duration (default 1m0s)
  -database string
        Cloud Spanner Database
  -instance string
        Cloud Spanner Instance
  -project string
        GCP Project
  -query string
        SQL
```

## Example

```
# It is strongly recommended to execute an SQL statement that returns a single row of results, such as `COUNT(*)`
$ spanner-loader -c 10 -d 5m0s -project myproject -instance test-instance -database test-database -query "SELECT COUNT(*) FROM Singers, Albums, Songs"
```
