defmodule MessageBroker.Polyglot do
  use GenServer

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: :polyglot)
  end

  def init(_opts) do
    {:ok, %{}}
  end

  def assign_protocol(socket) do
    MessageBroker.TerminalHandler.write_line(socket, {:ok, "Do you wish to use tcp or mqtt?"})

    with {:ok, data} <- MessageBroker.TerminalHandler.read_line(socket),
         protocol <- String.trim(data),
         :ok <- check_and_store_protocol(socket, protocol) do
      {:ok, "Successfully assigned protocol."}
    end
  end

  def check_and_store_protocol(socket, protocol) when protocol in ["tcp", "mqtt"] do
    GenServer.call(:polyglot, {:store_protocol, socket, "tcp"})
  end
  def check_and_store_protocol(_socket, _protocol), do: {:error, :unknown_protocol}

  def handle_call({:store_protocol, socket, protocol}, _from, state) do
    {:reply, :ok, Map.put(state, socket, protocol)}
  end

  def get_protocol(socket) do
    GenServer.call(:polyglot, {:get_protocol, socket})
  end

  def handle_call({:get_protocol, socket}, _from, state) do
    {:reply, Map.get(state, socket), state}
  end
end
