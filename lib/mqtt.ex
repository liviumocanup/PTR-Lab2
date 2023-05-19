defmodule MessageBroker.QoSManager do
  use GenServer

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: :qos_manager)
  end

  def init(_opts) do
    {:ok, %{
      to_be_ack: MessageBroker.StateStore.load_to_be_ack_map(),
      to_be_notified: MessageBroker.StateStore.load_to_be_notified_map(),
      publishers: MessageBroker.StateStore.load_publishers_map()
    }}
  end


  def pub(role, client, message_id) do
    GenServer.call(:qos_manager, {:publish, role, client, message_id})
  end

  # Publisher logic
  def pubrel(client, message_id) do
    GenServer.call(:qos_manager, {:pubrel, client, message_id})
  end

  # Subscriber logic
  def pubrec(subscriber, message_id) do
    GenServer.call(:qos_manager, {:pubrec, subscriber, message_id})
  end

  def pubcomp(subscriber, message_id) do
    GenServer.call(:qos_manager, {:pubcomp, subscriber, message_id})
  end

  def handle_call({:publish, role, client, message_id}, _from, state) do
    case role do
      :publisher ->
        name = MessageBroker.SubscriptionManager.get_name(client)
        updated_state = Map.put(state, :publishers, Map.put(state.publishers, message_id, {name, :published}))
        MessageBroker.StateStore.store_publishers_map(updated_state.publishers)
        {:reply, {:pubrec, message_id}, updated_state}
      :subscriber ->
        updated_state = Map.put(state, :to_be_ack, Map.put(state.to_be_ack, message_id, Map.get(state.to_be_ack, message_id, []) ++ [client]))
        updated_state = Map.put(updated_state, :to_be_notified, Map.put(state.to_be_notified, message_id, []))
        MessageBroker.StateStore.store_to_be_ack_map(updated_state.to_be_ack)
        MessageBroker.StateStore.store_to_be_notified_map(updated_state.to_be_notified)
        {:reply, {:publish, message_id}, updated_state}
      _ ->
        {:reply, {:error, :qos, :unknown_role}, state}
    end
  end

  def handle_call({:pubrel, client, message_id}, _from, state) do
    name = MessageBroker.SubscriptionManager.get_name(client)
    case Map.get(state.publishers, message_id) do
      {^name, :published} ->
        updated_state = Map.put(state, :publishers, Map.delete(state.publishers, message_id))
        MessageBroker.StateStore.store_publishers_map(updated_state.publishers)
        {:reply, {:pubcomp, message_id}, updated_state}
      _ ->
        {:reply, {:error, :qos, :no_pub}, state}
    end
  end

  def handle_call({:pubrec, subscriber, message_id}, _from, state) do
    name = MessageBroker.SubscriptionManager.get_name(subscriber)
    subscribers = Map.get(state.to_be_ack, message_id, [])
    if Enum.member?(subscribers, name) do
      updated_subscribers = List.delete(subscribers, name)
      updated_state = Map.put(state, :to_be_ack, Map.put(state.to_be_ack, message_id, updated_subscribers))
      updated_state = Map.put(updated_state, :to_be_notified, Map.put(state.to_be_notified, message_id, Map.get(state.to_be_notified, message_id, []) ++ [name]))
      MessageBroker.StateStore.store_to_be_ack_map(updated_state.to_be_ack)
      MessageBroker.StateStore.store_to_be_notified_map(updated_state.to_be_notified)
      {:reply, {:pubrel, message_id}, updated_state}
    else
      {:reply, {:error, :qos, :no_sub}, state}
    end
  end

  def handle_call({:pubcomp, subscriber, message_id}, _from, state) do
    name = MessageBroker.SubscriptionManager.get_name(subscriber)
    subscribers = Map.get(state.to_be_notified, message_id, [])
    if Enum.member?(subscribers, name) do
      updated_subscribers = List.delete(subscribers, name)
      updated_state = Map.put(state, :to_be_notified, Map.put(state.to_be_notified, message_id, updated_subscribers))
      MessageBroker.StateStore.store_to_be_notified_map(updated_state.to_be_notified)
      {:reply, :ok, updated_state}
    else
      {:reply, {:error, :qos, :no_comp}, state}
    end
  end
end
