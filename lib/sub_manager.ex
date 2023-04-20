defmodule MessageBroker.SubscriptionManager do
  use GenServer

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: :subscription_manager)
  end

  def init(_opts) do
    {:ok, %{topics: %{}, publishers: %{}, pub_sub: %{}}}
  end

  def subscribe_to_topic(client, topic) do
    GenServer.call(:subscription_manager, {:subscribe, client, topic})
  end

  def subscribe_to_publisher(client, publisher) do
    GenServer.call(:subscription_manager, {:subscribe_to_publisher, client, publisher})
  end

  def unsubscribe(client, topic) do
    GenServer.call(:subscription_manager, {:unsubscribe, client, topic})
  end

  def unsubscribe_from_publisher(client, publisher) do
    GenServer.call(:subscription_manager, {:unsubscribe_from_publisher, client, publisher})
  end

  def register_publisher(client, name) do
    GenServer.call(:subscription_manager, {:register, client, name})
  end

  def publish(client, topic, message) do
    GenServer.call(:subscription_manager, {:publish, client, topic, message})
  end

  def handle_call({:subscribe, client, topic}, _from, state) do
    topics = Map.get(state, :topics, %{})
    subscribers_to_topic = Map.get(topics, topic, [])

    if Enum.member?(subscribers_to_topic, client) do
      {:reply, {:error, :sub_manager, :already_subscribed}, state}
    else
      new_topic = Map.put(topics, topic, [client | subscribers_to_topic])
      new_state = Map.put(state, :topics, new_topic)
      {:reply, :ok, new_state}
    end
  end

  def handle_call({:subscribe_to_publisher, subscriber, publisher}, _from, state) do
    if Map.get(state.publishers, publisher, nil) == nil do
      {:reply, {:error, :sub_manager, :publisher_not_found}, state}
    else
      publisher_subscribers = Map.get(state.pub_sub, publisher, [])

      if Enum.member?(publisher_subscribers, subscriber) do
        {:reply, {:error, :sub_manager, :already_subscribed_to_publisher}, state}
      else
        new_state = %{state | pub_sub: Map.put(state.pub_sub, publisher, [subscriber | publisher_subscribers])}
        {:reply, :ok, new_state}
      end
    end
  end

  def handle_call({:unsubscribe, client, topic}, _from, state) do
    topics = Map.get(state, :topics, %{})
    subscribers_to_topic = Map.get(topics, topic, [])

    if Enum.member?(subscribers_to_topic, client) do
      new_subscribers = Enum.reject(subscribers_to_topic, &(&1 == client))
      new_topic = Map.put(topics, topic, new_subscribers)
      new_state = Map.put(state, :topics, new_topic)
      {:reply, :ok, new_state}
    else
      {:reply, {:error, :sub_manager, :not_subscribed}, state}
    end
  end

  def handle_call({:unsubscribe_from_publisher, subscriber, publisher}, _from, state) do
    if Map.get(state.publishers, publisher, nil) == nil do
      {:reply, {:error, :sub_manager, :publisher_not_found}, state}
    else
      publisher_subscribers = Map.get(state.pub_sub, publisher, [])

      if Enum.member?(publisher_subscribers, subscriber) do
        new_subscribers = Enum.reject(publisher_subscribers, &(&1 == subscriber))
        new_state = %{state | pub_sub: Map.put(state.pub_sub, publisher, new_subscribers)}
        {:reply, :ok, new_state}
      else
        {:reply, {:error, :sub_manager, :not_subscribed_publisher}, state}
      end
    end
  end

  def handle_call({:register, client, name}, _from, state) do
    publishers = Map.get(state, :publishers, %{})

    if Map.has_key?(publishers, name) do
      {:reply, {:error, :sub_manager, :already_registered}, state}
    else
      new_publishers = Map.put(publishers, name, client)
      new_state = Map.put(state, :publishers, new_publishers)
      {:reply, :ok, new_state}
    end
  end

  def handle_call({:publish, client, topic, message}, _from, state) do
    topic_subscribers = Map.get(state.topics, topic, [])

    pub_name = Enum.find_value(state.publishers, fn{key, value} -> value == client && key end)
    publisher_subscribers = Map.get(state.pub_sub, pub_name, [])

    all_subscribers = Enum.uniq(topic_subscribers ++ publisher_subscribers)

    Enum.each(all_subscribers, fn sub_socket -> send_message(sub_socket, pub_name, topic, message) end)
    {:reply, :ok, state}
  end

  defp send_message(socket, name, topic, message) do
    :gen_tcp.send(socket, "#{name} posted on topic [#{topic}]: #{inspect message}\r\n")
  end
end
