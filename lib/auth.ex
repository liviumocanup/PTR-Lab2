defmodule MessageBroker.Auth do
  use GenServer

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: :auth)
  end

  def init(_opts) do
    {:ok, %{}}
  end

  def assign_role(socket) do
    if MessageBroker.RoleManager.has_role?(socket) == false do
      write_line(socket, {:ok, "Do you wish to be a Publisher or Subscriber? PUB/SUB"})

      msg =
        with {:ok, data} <- read_line(socket),
        {:ok, role} <- MessageBroker.RoleManager.check_and_assign(socket, String.trim(data)),
        do: conclude(socket, role)

      write_line(socket, msg)
      case msg do
        {:error, :unknown, _} -> assign_role(socket)
        {:ok, _} ->
          case MessageBroker.Polyglot.assign_protocol(socket) do
            {:ok, reply} ->
              write_line(socket, {:ok, reply})
              MessageBroker.Client.serve(socket)
            {:error, _} -> assign_role(socket)
          end
      end
    end
  end

  def conclude(socket, role) do
    case role do
      :consumer ->
        write_line(socket, {:ok, "Please enter your subscriber name:"})
        with {:ok, name} <- read_line(socket),
        :ok <- MessageBroker.SubscriptionManager.update_subscriber(socket, String.trim(name)),
        do: {:ok, "Successfully assigned role and name."}
      :producer ->
        write_line(socket, {:ok, "Please enter your publisher name:"})
        with {:ok, name} <- read_line(socket),
        :ok <- MessageBroker.SubscriptionManager.update_publisher(socket, String.trim(name)),
        do: {:ok, "Successfully assigned role and name."}
    end
  end

  defp read_line(socket) do
    MessageBroker.TerminalHandler.read_line(socket)
  end

  defp write_line(socket, message) do
    MessageBroker.TerminalHandler.write_line(socket, message)
  end
end
