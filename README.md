# InfluxCopy

> A generic influx copy script with ability to update values of tags based on given information

## Installation

Make sure you have elixir 1.3+ installed

Grab the [influx-copy executable](https://github.com/Brightergy/influx-copy/raw/master/influx_copy) and you are good to go.

```shell
sudo wget https://github.com/Brightergy/influx-copy/raw/master/influx_copy -O /usr/local/bin
```

## Usage

```elixir
./influx_copy -S "https://user:pass@host:8086/db:measurement" -d "https://user:pass@host:333/db:measurement"

./influx_copy -S "http://a:a@localhost:8086/brighterlink_io_dev:power" -d "http://a:a@localhost:8086/brighterlink_io_test:power" -s 1469464091 -e 1469468091 -u "facility_id:1->3" -t "company_id,facility_id,device_id"
```

For more details on CLI args, check [influx_copy.ex](lib/influx_copy.ex#L5-L13)
