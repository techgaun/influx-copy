defmodule InfluxCopy.QueryBuilder do
  @moduledoc """
  A module to create influxdb query
  """

  @doc """
  ## Examples

      iex> InfluxCopy.QueryBuilder.create_query([selection: "gen,grid", measurement: "power", start_time: 1469453618, end_time: 1469463618])
      "select gen,grid from power where time >= 1469453618s and time <= 1469463618s"

      iex> InfluxCopy.QueryBuilder.create_query([selection: "gen,grid", measurement: "power", start_time: 1469453618])
      "select gen,grid from power where time >= 1469453618s"

      iex> InfluxCopy.QueryBuilder.create_query([selection: "gen,grid", measurement: "power", end_time: 1469463618])
      "select gen,grid from power where time <= 1469463618s"

      iex> InfluxCopy.QueryBuilder.create_query([measurement: "power"])
      "select * from power"
  """
  def create_query(opts \\ []) do
    selection = opts[:selection] || "*"
    measurement = opts[:measurement]
    start_time = opts[:start_time]
    end_time = opts[:end_time]

    "select #{selection} from #{measurement}"
    |> build_where(start_time, end_time)
  end

  def build_selection(nil), do: "*"

  def build_selection(selection) do
    selection
    |> String.split(",")
    |> Enum.map_join(", ")
  end

  def build_where(query, start_time, end_time) do
    query
    |> build_where_time(start_time, ">=")
    |> build_where_time(end_time, "<=")
  end

  def build_where_time(query, time, operator) do
    case is_integer(time) do
      true ->
        case String.contains?(query, "where time") do
          true ->
            "#{query} and time #{operator} #{time}s"

          false ->
            "#{query} where time #{operator} #{time}s"
        end

      false ->
        query
    end
  end
end
