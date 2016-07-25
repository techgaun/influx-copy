defmodule InfluxCopy do
  def main(args \\ []) do
    {opts, _, _} = OptionParser.parse(args,
      switches: [start: :integer, end: :integer, src: :string, dest: :string, update_tags: :string],
      aliases: [s: :start, e: :end, S: :src, d: :dest, u: :update_tags]
      )
    IO.puts "Not implemented yet"
  end
end
