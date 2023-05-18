defmodule MessageBroker.MQTThandler do
  use Tortoise.Handler

  alias Tortoise.Connection

  @spec start_link(any, any) :: none
  def start_link(opts, id) do
    client_id = Keyword.get(opts, :client_id, id)

    {server, opts} = Keyword.pop(opts, :server, 'localhost:1883')

    Connection.start_link(
      client_id: client_id,
      handler: {__MODULE__, %{client_id: client_id}},
      server: server,
      opts: opts
    )
  end

  # Called when the MQTT connection is established
  def init(args) do
    {:ok, args}
  end

  # Send an MQTT message
  @spec send_message(atom | binary, binary, nil | binary) ::
          :ok | {:error, :unknown_connection} | {:ok, reference}
  def send_message(client_id, topic, message) do
    Tortoise.publish(client_id, topic, message)
  end

  # Subscribe to an MQTT topic
  def subscribe(client_id, topic) do
    Tortoise.subscribe(client_id, topic)
  end

  # Unsubscribe from an MQTT topic
  def unsubscribe(client_id, topic) do
    Tortoise.unsubscribe(client_id, topic)
  end

  # Handle an MQTT message
  def handle_message({:publish, _packet_id, topic, message, _opts}, state) do
    IO.puts("Received message on topic #{topic}: #{message}")
    {:noreply, state}
  end

  # Handle other MQTT packets
  def handle_info({:tortoise_event, _event}, state) do
    {:noreply, state}
  end

  # Handle connection termination
  def terminate(:shutdown, _args) do
    :ok
  end
end
