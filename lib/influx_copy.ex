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
end
