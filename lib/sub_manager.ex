defmodule MessageBroker.SubscriptionManager do
  use GenServer

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: :subscription_manager)
  end

  def init(_opts) do
    publishers = MessageBroker.StateStore.load_publishers()
    subscribers = MessageBroker.StateStore.load_subscribers()
    topics = MessageBroker.StateStore.load_topics()
    pub_sub = MessageBroker.StateStore.load_pub_sub()
    {:ok, %{topics: topics, publishers: publishers, pub_sub: pub_sub, subscribers: subscribers}}
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

  def update_publisher(client, name) do
    GenServer.call(:subscription_manager, {:update_publisher, client, name})
  end

  def update_subscriber(client, name) do
    GenServer.call(:subscription_manager, {:update_subscriber, client, name})
  end

  def publish(client, topic, message) do
    GenServer.call(:subscription_manager, {:publish, client, topic, message})
  end

  def publish(message) do
    GenServer.call(:subscription_manager, {:publish, message})
  end

  def ack_message(client, message_id) do
    GenServer.call(:subscription_manager, {:ack, client, message_id})
  end

  def next_message(client) do
    GenServer.call(:subscription_manager, {:ack_next, client})
  end

  def get_name(client) do
    GenServer.call(:subscription_manager, {:get_name, client})
  end

  def get_port(name, role) do
    GenServer.call(:subscription_manager, {:get_port, name, role})
  end

  def handle_call({:get_name, client}, _from, state) do
    role = MessageBroker.RoleManager.get_role(client)
    case role do
      :consumer -> {:reply, Enum.find_value(state.subscribers, fn{key, value} -> value == client && key end), state}
      :producer -> {:reply, Enum.find_value(state.publishers, fn{key, value} -> value == client && key end), state}
      _ -> {:reply, {:error, :sub_manager, :unknown_role}, state}
    end
  end

  def handle_call({:get_port, name, role}, _from, state) do
    case role do
      :consumer -> {:reply, Map.get(state.subscribers, name, nil), state}
      :producer -> {:reply, Map.get(state.publishers, name, nil), state}
      _ -> {:reply, {:error, :sub_manager, :unknown_role}, state}
    end
  end

  def handle_call({:subscribe, client, topic}, _from, state) do
    topics = Map.get(state, :topics, %{})
    subscribers_to_topic = Map.get(topics, topic, [])
    sub_name = Enum.find_value(state.subscribers, fn{key, value} -> value == client && key end)

    if Enum.member?(subscribers_to_topic, sub_name) do
      {:reply, {:error, :sub_manager, :already_subscribed}, state}
    else
      new_topic = Map.put(topics, topic, [sub_name | subscribers_to_topic])
      new_state = Map.put(state, :topics, new_topic)
      MessageBroker.StateStore.store_topics(new_topic)
      {:reply, :ok, new_state}
    end
  end

  def handle_call({:subscribe_to_publisher, subscriber, publisher}, _from, state) do
    if Map.get(state.publishers, publisher, nil) == nil do
      {:reply, {:error, :sub_manager, :publisher_not_found}, state}
    else
      publisher_subscribers = Map.get(state.pub_sub, publisher, [])
      sub_name = Enum.find_value(state.subscribers, fn{key, value} -> value == subscriber && key end)

      if Enum.member?(publisher_subscribers, sub_name) do
        {:reply, {:error, :sub_manager, :already_subscribed_to_publisher}, state}
      else
        new_pub_sub = Map.put(state.pub_sub, publisher, [sub_name | publisher_subscribers])
        new_state = %{state | pub_sub: new_pub_sub}
        MessageBroker.StateStore.store_pub_sub(new_pub_sub)
        {:reply, :ok, new_state}
      end
    end
  end

  def handle_call({:unsubscribe, client, topic}, _from, state) do
    topics = Map.get(state, :topics, %{})
    subscribers_to_topic = Map.get(topics, topic, [])
    sub_name = Enum.find_value(state.subscribers, fn{key, value} -> value == client && key end)

    if Enum.member?(subscribers_to_topic, sub_name) do
      new_subscribers = Enum.reject(subscribers_to_topic, &(&1 == sub_name))
      new_topic = Map.put(topics, topic, new_subscribers)
      new_state = Map.put(state, :topics, new_topic)
      MessageBroker.StateStore.store_topics(new_topic)
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
      sub_name = Enum.find_value(state.subscribers, fn{key, value} -> value == subscriber && key end)

      if Enum.member?(publisher_subscribers, sub_name) do
        new_subscribers = Enum.reject(publisher_subscribers, &(&1 == sub_name))
        new_pub_sub = Map.put(state.pub_sub, publisher, new_subscribers)
        new_state = %{state | pub_sub: new_pub_sub}
        MessageBroker.StateStore.store_pub_sub(new_pub_sub)
        {:reply, :ok, new_state}
      else
        {:reply, {:error, :sub_manager, :not_subscribed_publisher}, state}
      end
    end
  end

  def handle_call({:update_publisher, client, name}, _from, state) do
    updated_pubs = Map.put(state.publishers, name, client)
    new_state = Map.put(state, :publishers, updated_pubs)
    MessageBroker.StateStore.store_publishers(updated_pubs)
    {:reply, :ok, new_state}
  end

  def handle_call({:update_subscriber, client, name}, _from, state) do
    updated_subs = Map.put(state.subscribers, name, client)
    new_state = Map.put(state, :subscribers, updated_subs)
    MessageBroker.StateStore.store_subscribers(updated_subs)
    {:reply, :ok, new_state}
  end

  def handle_call({:publish, client, topic, message}, _from, state) do
    topic_subscribers = Map.get(state.topics, topic, [])

    pub_name = Enum.find_value(state.publishers, fn{key, value} -> value == client && key end)
    publisher_subscribers = Map.get(state.pub_sub, pub_name, [])

    all_subscribers = Enum.uniq(topic_subscribers ++ publisher_subscribers)

    message_id = MessageBroker.MessageIDGenerator.generate_id(pub_name, message)
    IO.inspect(message_id)
    MessageBroker.MessageStore.store_regular_message(topic, message, pub_name, message_id)

    Enum.each(all_subscribers, fn sub_name ->
      MessageBroker.SubscriberQueueManager.add_message_to_queue(sub_name, topic, message_id)
      MessageBroker.QoSManager.pub(:subscriber, sub_name, message_id)
    end)

    {:reply, {:ok, message_id}, state}
  end

  def handle_call({:publish, message}, _from, state) do
    topic = "db_change"
    topic_subscribers = Map.get(state.topics, topic, [])

    message_id = MessageBroker.MessageIDGenerator.generate_id("db", message)
    IO.inspect(message_id)
    MessageBroker.MessageStore.store_regular_message(topic, message, "db", message_id)

    Enum.each(topic_subscribers, fn sub_name ->
      MessageBroker.SubscriberQueueManager.add_message_to_queue(sub_name, topic, message_id)
      MessageBroker.QoSManager.pub(:subscriber, sub_name, message_id)
    end)

    {:reply, {:ok, message_id}, state}
  end

  def handle_call({:ack, subscriber, message_id}, _from, state) do
    sub_name = Enum.find_value(state.subscribers, fn{key, value} -> value == subscriber && key end)

    with {:ok, topic} <- MessageBroker.SubscriberQueueManager.get_topic_by_message_id(sub_name, message_id),
      :ok <- MessageBroker.SubscriberQueueManager.ack_message(sub_name, topic, message_id),
      {:ok, {next_topic, next_message_id}} <- MessageBroker.SubscriberQueueManager.get_current_message(sub_name),
      {:ok, {publisher, message}} <- MessageBroker.MessageStore.get_message(next_topic, next_message_id) do
        {:reply, {:ok, "#{publisher} posted on topic [#{next_topic}] message with id #{inspect next_message_id}: #{message}\r\n"}, state}
    else
      error -> {:reply, error, state}
    end
  end

  def handle_call({:ack_next, subscriber}, _from, state) do
    sub_name = Enum.find_value(state.subscribers, fn{key, value} -> value == subscriber && key end)
    with {:ok, {_, message_id}} <- MessageBroker.SubscriberQueueManager.get_current_message(sub_name),
    do: ack_message(subscriber, message_id)
  end
end
