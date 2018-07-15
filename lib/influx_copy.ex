defmodule InfluxCopy do
  @moduledoc """
  Module entrypoint for influx copy script

  cli args you can pass

    --start/-s unix_timestamp
    --end/-e unix_timestamp
    --src/-S https://user:pass@host:port/db:measurement
    --dest/-d https://user:pass@host:port/db:measurement
    --tags/-t facility_id,device_id,company_id
    --update_tags/-u facility_id:1->3
    --fields/-f grid (defaults to * i.e. all fields are copied over)
  """

  alias InfluxCopy.{SrcConn, DestConn, QueryBuilder}

  @conn_error "connection string invalid. Format example: https://user:pass@host:port/db:measurement"

  def main(args \\ []) do
    {opts, _, _} =
      OptionParser.parse(
        args,
        switches: [
          start: :integer,
          end: :integer,
          src: :string,
          dest: :string,
          tags: :string,
          update_tags: :string,
          fields: :string
        ],
        aliases: [s: :start, e: :end, S: :src, d: :dest, t: :tags, u: :update_tags, f: :fields]
      )

    src = parse_conn(opts[:src])
    dest = parse_conn(opts[:dest])

    errors = if is_nil(src), do: ["Source #{@conn_error}"], else: []

    errors = if is_nil(dest), do: ["Destination #{@conn_error}" | errors], else: errors

    case Enum.count(errors) do
      0 ->
        IO.puts("Performing the copy...")

        Supervisor.start_link(
          [SrcConn.child_spec()],
          strategy: :one_for_one
        )

        Supervisor.start_link(
          [DestConn.child_spec()],
          strategy: :one_for_one
        )

        {src_config, src_db, src_measurement} = get_influx_config(SrcConn.config(), src)
        {dest_config, dest_db, dest_measurement} = get_influx_config(DestConn.config(), dest)
        :ok = Application.put_env(:influx_copy, SrcConn, src_config)
        :ok = Application.put_env(:influx_copy, DestConn, dest_config)

        query_opts = [
          selection: opts[:fields],
          measurement: src_measurement,
          start_time: opts[:start],
          end_time: opts[:end]
        ]

        select_query = QueryBuilder.create_query(query_opts)

        source_data =
          select_query
          |> SrcConn.query(database: src_db, precision: :second, timeout: 60_000)

        case source_data do
          %{results: [%{series: [%{columns: columns, values: values}]}]} ->
            values
            |> Enum.each(fn x ->
              data_to_write =
                List.zip([columns | [x]])
                |> Enum.into(%{})
                |> update_tag_value(opts[:update_tags])

              timestamp = data_to_write["time"]

              {tags, fields} =
                Map.delete(data_to_write, "time")
                |> split_fields_tags(opts[:tags])

              influx_data = %{
                database: dest_db,
                points: [
                  %{
                    measurement: dest_measurement,
                    fields: fields,
                    tags: tags,
                    timestamp: timestamp
                  }
                ]
              }

              [influx_data]
              |> DestConn.write(database: dest_db, precision: :second, async: false)
            end)

          %{results: [%{}]} ->
            IO.puts("No data in the given timeframe on source connection...")

          _ ->
            IO.puts("Unknown issue occurred while reading from source...")
        end

      _ ->
        errors
        |> Enum.each(fn x ->
          IO.puts(x)
        end)
    end
  end

  def parse_conn(str) when is_bitstring(str) do
    re = ~r/(.*):\/\/(.*):(.*)@(.*):(.*)\/(.*):(.*)/

    case Regex.run(re, str) do
      [_h | t] = [_, _scheme, _user, _pass, _host, _port, _db, _measurement] ->
        t

      _ ->
        nil
    end
  end

  def parse_conn(_), do: nil

  def get_influx_config(config, conn_list) do
    [scheme, user, pass, host, port, db, measurement] = conn_list

    new_config =
      config
      |> Keyword.put(:host, host)
      |> Keyword.put(:scheme, scheme)
      |> Keyword.put(:port, port)
      |> Keyword.put(:auth, username: user, password: pass)

    {new_config, db, measurement}
  end

  @doc """
  ## Examples

      iex> InfluxCopy.update_tag_value(%{"a" => 1}, nil)
      %{"a" => 1}

      iex> InfluxCopy.update_tag_value(%{"tag_id" => "1"}, "tag_id:1->2")
      %{"tag_id" => "2"}
  """
  def update_tag_value(data, tags) when is_bitstring(tags) do
    tags
    |> String.split(",")
    |> Enum.reduce(data, fn x, acc ->
      [tag | [mod]] = String.split(x, ":")
      [from | [to]] = String.split(mod, "->")

      if from === Map.get(acc, tag) do
        Map.put(acc, tag, to)
      else
        acc
      end
    end)
  end

  def update_tag_value(data, _), do: data

  def split_fields_tags(data, tags) when is_bitstring(tags) do
    tags =
      tags
      |> String.split(",")

    tags_map =
      tags
      |> Enum.reduce(%{}, fn x, acc ->
        Map.put(acc, x, Map.get(data, x))
      end)

    fields_map =
      tags
      |> Enum.reduce(data, fn x, acc ->
        Map.delete(acc, x)
      end)

    {tags_map, fields_map}
  end

  def split_fields_tags(data, _), do: {%{}, data}
end
