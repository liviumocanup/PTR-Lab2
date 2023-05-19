defmodule MessageBroker.RoleManager do
  use GenServer

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: :role_manager)
  end

  def init(_opts) do
    {:ok, %{}}
  end

  def assign_role(client, role) do
    GenServer.call(:role_manager, {:assign_role, client, role})
  end

  def get_role(client) do
    GenServer.call(:role_manager, {:get_role, client})
  end

  def get_readable_role(client) do
    role = get_role(client)
    case role do
      :producer -> "PUBLISHER"
      :consumer -> "SUBSCRIBER"
      _ -> "NO ROLE"
    end
  end

  def check_role(client, required_role) do
    role = get_role(client)
    role == required_role
  end

  def has_role?(client) do
    get_role(client) != :no_role
  end

  def check_and_assign(client, input) do
    status = case input do
      "PUB" -> assign_role(client, :producer)
      "SUB" -> assign_role(client, :consumer)
      _ -> {:error, :unknown, "role #{inspect input}."}
    end
    status
  end

  def handle_call({:assign_role, client, role}, _from, state) do
    new_state = Map.put(state, client, role)
    {:reply, {:ok, role}, new_state}
  end

  def handle_call({:get_role, client}, _from, state) do
    role = Map.get(state, client, :no_role)
    {:reply, role, state}
  end
end
