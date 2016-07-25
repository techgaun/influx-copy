defmodule InfluxCopy do
  @conn_error "connection string invalid. Format example: https://user:pass@host:port/db:measurement"
  def main(args \\ []) do
    {opts, _, _} = OptionParser.parse(args,
      switches: [start: :integer, end: :integer, src: :string, dest: :string, update_tags: :string],
      aliases: [s: :start, e: :end, S: :src, d: :dest, u: :update_tags]
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
end
