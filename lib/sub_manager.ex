defmodule MessageBroker.SubscriptionManager do
  use GenServer

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: :subscription_manager)
  end

  def init(_opts) do
    {:ok, %{topics: %{}, publishers: %{}}}
  end

  def subscribe(client, topic) do
    GenServer.call(:subscription_manager, {:subscribe, client, topic})
  end

  def unsubscribe(client, topic) do
    GenServer.call(:subscription_manager, {:unsubscribe, client, topic})
  end

  def subscribe_to_publisher(subscriber, publisher) do
    GenServer.call(:subscription_manager, {:subscribe_to_publisher, subscriber, publisher})
  end

  def publish(client, topic, message) do
    GenServer.call(:subscription_manager, {:publish, client, topic, message})
  end

  def handle_call({:subscribe, client, topic}, _from, state) do
    subscribers = Map.get(state, topic, [])

    if Enum.member?(subscribers, client) do
      {:reply, {:error, :sub_manager, :already_subscribed}, state}
    else
      new_state = Map.put(state, topic, [client | subscribers])
      {:reply, :ok, new_state}
    end
  end

  def handle_call({:unsubscribe, client, topic}, _from, state) do
    subscribers = Map.get(state, topic, [])

    if Enum.member?(subscribers, client) do
      new_subscribers = Enum.reject(subscribers, &(&1 == client))
      new_state = Map.put(state, topic, new_subscribers)
      {:reply, :ok, new_state}
    else
      {:reply, {:error, :sub_manager, :not_subscribed}, state}
    end
  end

  def handle_call({:subscribe_to_publisher, subscriber, publisher}, _from, state) do
    publisher_subscribers = Map.get(state.publishers, publisher, [])

    if Enum.member?(publisher_subscribers, subscriber) do
      {:reply, {:error, :sub_manager, :already_subscribed_to_publisher}, state}
    else
      new_state = %{state | publishers: Map.put(state.publishers, publisher, [subscriber | publisher_subscribers])}
      {:reply, :ok, new_state}
    end
  end

  def handle_call({:publish, client, topic, message}, _from, state) do
    topic_subscribers = Map.get(state.topics, topic, [])
    publisher_subscribers = Map.get(state.publishers, client, [])
    all_subscribers = Enum.uniq(topic_subscribers ++ publisher_subscribers)

    Enum.each(all_subscribers, fn sub_socket -> send_message(sub_socket, topic, message) end)
    {:reply, :ok, state}
  end

  defp send_message(socket, topic, message) do
    :gen_tcp.send(socket, "#{topic}: #{message}\r\n")
  end
end
