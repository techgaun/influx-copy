defmodule InfluxCopy do
  @moduledoc """
  Module entrypoint for influx copy script
  """

  alias InfluxCopy.{SrcConn, DestConn, QueryBuilder}
  require Logger
  @conn_error "connection string invalid. Format example: https://user:pass@host:port/db:measurement"
  def main(args \\ []) do
    {opts, _, _} = OptionParser.parse(args,
      switches: [start: :integer, end: :integer, src: :string, dest: :string, update_tags: :string, fields: :string],
      aliases: [s: :start, e: :end, S: :src, d: :dest, u: :update_tags, f: :fields]
      )
    src = parse_conn(opts[:src])
    dest = parse_conn(opts[:dest])
    errors = []
    if src |> is_nil do
      errors = ["Source #{@conn_error}" | errors]
    end
    if dest |> is_nil do
      errors = ["Destination #{@conn_error}" | errors]
    end

    case Enum.count(errors) do
      0 ->
        IO.puts "Performing the copy..."
        Supervisor.start_link(
          [ SrcConn.child_spec ],
          strategy: :one_for_one
        )
        Supervisor.start_link(
          [ DestConn.child_spec ],
          strategy: :one_for_one
        )

        {src_config, src_db, src_measurement} = get_influx_config(SrcConn.config, src)
        {dest_config, dest_db, dest_measurement} = get_influx_config(DestConn.config, dest)
        :ok = Application.put_env(:influx_copy, SrcConn, src_config)
        :ok = Application.put_env(:influx_copy, DestConn, dest_config)

        query_opts = [
          selection: opts[:fields],
          measurement: src_measurement,
          start_time: opts[:start],
          end_time: opts[:end],
        ]
        select_query = QueryBuilder.create_query(query_opts)
        source_data = select_query
          |> SrcConn.query(database: src_db, precision: :seconds)

        case source_data do
          %{results: [%{series: [%{columns: columns, values: values}]}]} ->
            values
            |> Enum.each(fn x ->
              data_to_write = List.zip([columns | [x]])
                |> Enum.into(%{})
                |> update_tag_value(opts[:update_tags])
              Logger.warn inspect data_to_write
            end)

          %{results: [%{}]} ->
            IO.puts "No data in the given timeframe on source connection..."

          _ ->
            IO.puts "Unknown issue occurred while reading from source..."
        end

      _ ->
        errors
        |> Enum.each(fn x ->
          IO.puts x
        end)
    end
    IO.puts "Not implemented yet"
  end

  def parse_conn(str) when is_bitstring(str) do
    re = ~r/(.*):\/\/(.*):(.*)@(.*):(.*)\/(.*):(.*)/
    case Regex.run(re, str) do
      [h | t] = [_, scheme, user, pass, host, port, db, measurement] ->
        t
      _ ->
        nil
    end
  end
  def parse_conn(_), do: nil

  def get_influx_config(config, conn_list) do
    [scheme, user, pass, host, port, db, measurement] = conn_list
    new_config = config
      |> Keyword.put(:host, host)
      |> Keyword.put(:scheme, scheme)
      |> Keyword.put(:port, port)
      |> Keyword.put(:auth, [username: user, password: pass])
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
    |> Enum.reduce(data, fn (x, acc) ->
      [tag | [mod]] = String.split(x, ":")
      [from | [to]] = String.split(mod, "->")
      if from === Map.get(acc, tag) do
        acc = Map.put(acc, tag, to)
      end
      acc
    end)
  end
  def update_tag_value(data, _), do: data
end
