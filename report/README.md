# FAF.PTR16.1 -- Project 1
> **Performed by:** Mocanu Liviu, group FAF-203
> **Verified by:** asist. univ. Alexandru Osadcenco

## Minimal Features

**Task 1** -- The message broker provides the ability to subscribe to publishers (if you are a consumer) and publish messages for subscribed consumers to receive (if you are a publisher);

```elixir
defmodule MessageBroker.Server do
  require Logger

  def accept(port) do
    {:ok, socket} = :gen_tcp.listen(port, [:binary, packet: :line, active: false, reuseaddr: true])
    Logger.info "Accepting connections on port #{port}"
    loop_acceptor(socket)
  end

  defp loop_acceptor(socket) do
    {:ok, client} = :gen_tcp.accept(socket)
    {:ok, pid} = Task.Supervisor.start_child(MessageBroker.TaskSupervisor, fn -> MessageBroker.Auth.assign_role(client) end)
    :ok = :gen_tcp.controlling_process(client, pid)
    loop_acceptor(socket)
  end
end

```

The module Server that is responsible for starting the Broker on a given port as well as listening for new clients that could try to connect.

We start the Message Broker on a port we provide using:

```bash
$ PORT=4045 mix run --no-halt
```

The default PORT is 4040 if not specified.

```elixir
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
```

In case the server found a new user, a worker Auth is responsible for the initial communication (Deciding the role, name and protocol of the user). After all of that has been decided, Auth concludes and makes a Client server this connections further.

```elixir
defmodule MessageBroker.Client do
  def serve(socket, buffer \\ "") do
    {:ok, data} = MessageBroker.TerminalHandler.read_line(socket)
    command = buffer <> data

    cond do
      String.contains?(command, "END\r\n") ->
        # complete command received, parse it
        msg =
          with {:ok, cmd} <- MessageBroker.Command.parse(command),
            do: MessageBroker.Command.run(socket, cmd)

        MessageBroker.TerminalHandler.write_line(socket, msg)
        serve(socket)

      true ->
        # incomplete command, continue buffering
        serve(socket, command)
    end
  end
end
```

By serving the connection, I mean listening for any input or providing any output necessary to this connection on an infinite loop until connection closed.

```elixir
defmodule MessageBroker.Command do
  @doc ~S"""
  Parses the given `line` into a command.
  """

  def parse(command) do
    command =
      command
      |> String.trim("\r\n")
      |> String.split("\r\n", trim: true)

    case command do
      ["START", "topic:" <> topic, "message:" <> message, "END"] ->
        {:ok, {:publish, String.trim(topic), String.trim(message)}}

      _ ->
        parts = List.first(command) |> String.split("|")

        case parts do
          ["SUB" | [topic]] -> {:ok, {:subscribe_topic, String.trim(topic)}}
          ["SUB@" | [name]] -> {:ok, {:subscribe_publisher, String.trim(name)}}
          ["UNSUB" | [topic]] -> {:ok, {:unsubscribe, String.trim(topic)}}
          ["UNSUB@" | [name]] -> {:ok, {:unsubscribe_publisher, String.trim(name)}}
          ["PUB" | [topic, message]] -> {:ok, {:publish, String.trim(topic), String.trim(message)}}
          ["ACK" | [ref]] -> {:ok, {:ack, String.trim(ref)}}
          ["PUBREL" | [ref]] -> {:ok, {:pubrel, String.trim(ref)}}
          ["PUBREC" | [ref]] -> {:ok, {:pubrec, String.trim(ref)}}
          ["PUBCOMP" | [ref]] -> {:ok, {:pubcomp, String.trim(ref)}}
          _ ->
            {:error, :unknown, "Command #{inspect command}."}
        end
    end
  end

  def run(client, {:subscribe_topic, topic}) do
    if check_role(client, :consumer) do
      status = MessageBroker.SubscriptionManager.subscribe_to_topic(client, topic)

      case status do
        :ok -> {:ok, "Subscribed to topic: #{inspect topic}."}
        _ -> status
      end
    else
      {:error, :unauthorized, "subscribe"}
    end
  end

  def run(client, {:subscribe_publisher, name}) do
    if check_role(client, :consumer) do
      status = MessageBroker.SubscriptionManager.subscribe_to_publisher(client, name)

      case status do
        :ok -> {:ok, "Subscribed to publisher: #{inspect name}."}
        _ -> status
      end
    else
      {:error, :unauthorized, "subscribe"}
    end
  end

  def run(client, {:unsubscribe, topic}) do
    if check_role(client, :consumer) do
      status = MessageBroker.SubscriptionManager.unsubscribe(client, topic)

      case status do
        :ok -> {:ok, "Unsubscribed from topic: #{inspect topic}."}
        _ -> status
      end
    else
      {:error, :unauthorized, "unsubscribe"}
    end
  end

  def run(client, {:unsubscribe_publisher, name}) do
    if check_role(client, :consumer) do
      status = MessageBroker.SubscriptionManager.unsubscribe_from_publisher(client, name)

      case status do
        :ok -> {:ok, "Unsubscribed from publisher: #{inspect name}."}
        _ -> status
      end
    else
      {:error, :unauthorized, "unsubscribe"}
    end
  end


  def run(client, {:publish, topic, message}) do
    if check_role(client, :producer) do
      if verify_message(client, topic, message) do
        status = MessageBroker.SubscriptionManager.publish(client, topic, message)

        case status do
          {:ok, ref} ->
            with {:pubrec, ref} <- MessageBroker.QoSManager.pub(:publisher, client, ref),
              do: {:ok, "PUBREC #{inspect ref}."}
          _ -> status
        end
      else
        {:error, :unsendable}
      end
    else
      {:error, :unauthorized, "publish"}
    end
  end

  def run(client, {:pubrel, ref}) do
    status = MessageBroker.QoSManager.pubrel(client, ref)

    case status do
      {:pubcomp, id} -> {:ok, "PUBCOMP #{inspect id}."}
      _ -> status
    end
  end

  def run(client, {:pubcomp, ref}) do
    status = MessageBroker.QoSManager.pubcomp(client, ref)

    case status do
      :ok -> MessageBroker.SubscriptionManager.ack_message(client, ref)
      _ -> status
    end
  end

  def run(client, {:pubrec, ref}) do
    status = MessageBroker.QoSManager.pubrec(client, ref)

    case status do
      {:pubrel, id} -> {:ok, "PUBREL #{inspect id}."}
      _ -> status
    end
  end



  def run(client, {:ack, ref}) do
    if check_role(client, :consumer) do
      status = MessageBroker.SubscriptionManager.ack_message(client, ref)

      case status do
        :ok ->
          {:ok, "Acknowledged message #{inspect ref}."}
        _ -> status
      end
    else
      {:error, :unauthorized, "acknowledge"}
    end
  end



  def check_role(client, role) do
    MessageBroker.RoleManager.check_role(client, role)
  end

  def verify_message(client, topic, message) do
    reply = MessageBroker.DeadLetterChannel.check_message(client, topic, message)
    case reply do
      :ok -> true
      _ -> false
    end
  end

  def corrupted_command(client, command) do
    MessageBroker.DeadLetterChannel.store_unsendable(client, "Unknown command #{inspect command}")
  end
end
```

The Command module is first being called on detecting a line as input from the user and is responsible first of all for parsing and finding out whether the command that is being parsed is a valid one:

for SUB:
- SUB|topic
- UNSUB|topic
- SUB@|publisher
- UNSUB@|publisher

for PUB:
- PUB|topic|message

as well as the class-counterpart which does the same thing:
```bash
START
  topic: "topic"
  message: "message"
END
```

there are some other commands but the ones showcased are responsible for the base subscription/publishing functionality.
In order to be able to separate between Publisher/Subscriber I have a Role Manager module:

```elixir
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
```

Basically it is responsible for remembering the role of a certain user as well as provide the functionality for checking whether this user has enough rights to do a certain actions (SUBs are not allowed to publish for example). In order to actually send the messages and subscribe users to topics and other publishers, I have the Subscription Manager module:

```elixir
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
```

It is responsible for most of the business logic regarding subscribing, unsubscribing and publishing messages.

```elixir
def init(_opts) do
  publishers = MessageBroker.StateStore.load_publishers()
  subscribers = MessageBroker.StateStore.load_subscribers()
  topics = MessageBroker.StateStore.load_topics()
  pub_sub = MessageBroker.StateStore.load_pub_sub()
  {:ok, %{topics: topics, publishers: publishers, pub_sub: pub_sub, subscribers: subscribers}}
end
```

It stores as a map:
- Topics that exist, having the keys as topics and values array of subscriber names that are following it
- Publishers that are registered, having the keys as publisher names and values the socket they connected with
- Subscribers that are registered, having the keys as subscriber names and values the socket they connected with
- The relation between publishers and subscribers (subscription to publishers instead of specific topics), having the keys as publisher names and values array of subscriber names

Subscribing is pretty straight-forward, adding a user in the correct map and on publish, I combine the subscribers following the topic of the message and subscribers following the publisher of the message and that's who this message is adressed to:

```elixir
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
```

**Task 2** -- The message broker represents a dedicated TCP / UDP server.

As we saw in the entry point (Server module), the Message Broker starts listening to connections by opening a port utilising :gen_tcp as previously explained, thus being a TCP server:

```elixir
defmodule MessageBroker.Server do
  require Logger

  def accept(port) do
    {:ok, socket} = :gen_tcp.listen(port, [:binary, packet: :line, active: false, reuseaddr: true])
    Logger.info "Accepting connections on port #{port}"
    loop_acceptor(socket)
  end

  defp loop_acceptor(socket) do
    {:ok, client} = :gen_tcp.accept(socket)
    {:ok, pid} = Task.Supervisor.start_child(MessageBroker.TaskSupervisor, fn -> MessageBroker.Auth.assign_role(client) end)
    :ok = :gen_tcp.controlling_process(client, pid)
    loop_acceptor(socket)
  end
end
```

We start the Message Broker on a port we provide using:

```bash
$ PORT=4045 mix run --no-halt
```

The default PORT is 4040 if not specified.

**Task 3** -- The message broker allows for clients to connect via telnet / netcat;

Similar to Task 2, by being a TCP dedicated server, the Message Broker allows the clients to connect via telnet in Server module:

```elixir
defmodule MessageBroker.Server do
  require Logger

  def accept(port) do
    {:ok, socket} = :gen_tcp.listen(port, [:binary, packet: :line, active: false, reuseaddr: true])
    Logger.info "Accepting connections on port #{port}"
    loop_acceptor(socket)
  end

  defp loop_acceptor(socket) do
    {:ok, client} = :gen_tcp.accept(socket)
    {:ok, pid} = Task.Supervisor.start_child(MessageBroker.TaskSupervisor, fn -> MessageBroker.Auth.assign_role(client) end)
    :ok = :gen_tcp.controlling_process(client, pid)
    loop_acceptor(socket)
  end
end
```

In order to connect as a user using telnet we use:

```bash
$ telnet 127.0.0.1 4045
```

Where 4045 can be any other PORT the Message Broker currently runs on.

**Task 4** -- The message broker provides the ability to subscribe to multiple topics (if you are a consumer) and publish messages on different topics (if you are a publisher);

As we already established, the user role is being verified when trying to run the command if it was parsed succesfully, so that in the Subscription Manager we only deal with the business logic:

```elixir
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
```

Basically, I have a queue per topic and in order for the subscribers to see the messages, when it gets published, the message_id is stored in their queue, so that when they acknowledge the current message after reading, they immediately get the next one. This is being done with the MQTT-style assured delivery as we'll see in a bit, but basically for that, we store the topic of the message and the message_id in each user queue subscriber to it.

When the user needs to read the next message, using the topic we find the correct file (file per topic = queue per topic and they have the name as their respective topic for easier lookup) and then we search the message using its id which is the same. Here we have a lot more useful information such as the publisher who posted it and the message text itself.

A new topic is created both when subscribing to it and when publishing to it, that is because in case the topic we're trying to find doesn't exist, it gets immediately created.

**Task 5** -- The project has an executable that can run the message broker with a single click / command.

I have a docker-compose.yml file to satisfy this need, but first we have a look at the Dockerfile:

```Dockerfile
# Dockerfile
FROM elixir:1.12.3-alpine

WORKDIR /app

RUN mix local.hex --force && \
    mix local.rebar --force

# Install dependencies
COPY mix.exs mix.lock ./
RUN mix do deps.get, deps.compile

# Copy all application files
COPY . .

EXPOSE 4040

CMD ["mix", "run", "--no-halt"]
```

which exposes port 4040 for the Message Broker as it is considered the default in my case.

After that:
```yaml
version: '3'
services:
  message_broker:
    build:
      context: ./lab_2/message_broker
      dockerfile: Dockerfile
    ports:
      - 4040:4040

  my_app:
    build:
      context: ./lab 1
      dockerfile: Dockerfile
    depends_on:
      - message_broker
      - database_container

  database_container:
    image: alexburlacu/rtp-server:faf18x
    ports:
      - 4000:4000
```

And we can run this with one single command:
```bash
docker-compose up --build
```

## Main Features

**Task 1** -- The message broker provides the ability to unsubscribe to consumers;

This functionality was added together with the subscribing logic as it goes hand in hand and is present in the Subscription Manager:

```elixir
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
```

Basically, the subscribers simply get removed from the respective map (topics or pub_sub), thus they won't receive any more messages from it.

**Task 2** -- The message broker has a dead letter channel for messages deemed “unsendable”;

For this task I have a new module defined called DeadLetterChannel:

```elixir
defmodule MessageBroker.DeadLetterChannel do
  def check_message(client, topic, message) do
    max_length = 1000

    if message != "" && String.trim(topic) != "" && String.length(message) <= max_length do
      :ok
    else
      store_unsendable(client, "Post on topic: #{topic}, with message: #{message}")
      :unsendable
    end
  end

  def store_unsendable(client, message) do
    MessageBroker.MessageStore.store_message(client, message, :dead)
  end
end
```

Basically, since I verify the validity of a command in Command.parse, I know the commands are correct, thus all I have remained to check is the validity of the topic, message and its length which I do just that here. Otherwise it will be stored in a file in folder messages/dead describing the attempt of this dead letter.

**Task 3** -- The message broker allows for communication via structures / classes, serialized to be sent via network;

Firstly, all my commands were one-line commands. With the addition of communication via structures, they are considered multi-lined and must end with an END. The best example of this is:

```bash
START
  topic: topic
  message: message
END
```

Which is being accepted in the Client:

```elixir
defmodule MessageBroker.Client do
  def serve(socket, buffer \\ "") do
    {:ok, data} = MessageBroker.TerminalHandler.read_line(socket)
    command = buffer <> data

    cond do
      String.contains?(command, "END\r\n") ->
        # complete command received, parse it
        msg =
          with {:ok, cmd} <- MessageBroker.Command.parse(command),
            do: MessageBroker.Command.run(socket, cmd)

        MessageBroker.TerminalHandler.write_line(socket, msg)
        serve(socket)

      true ->
        # incomplete command, continue buffering
        serve(socket, command)
    end
  end
end
```

and passed to be parsed line-by-line in the Command module:

```elixir
defmodule MessageBroker.Command do
  @doc ~S"""
  Parses the given `line` into a command.
  """

  def parse(command) do
    command =
      command
      |> String.trim("\r\n")
      |> String.split("\r\n", trim: true)

    case command do
      ["START", "topic:" <> topic, "message:" <> message, "END"] ->
        {:ok, {:publish, String.trim(topic), String.trim(message)}}

      _ ->
        parts = List.first(command) |> String.split("|")

        case parts do
          ["SUB" | [topic]] -> {:ok, {:subscribe_topic, String.trim(topic)}}
          ["SUB@" | [name]] -> {:ok, {:subscribe_publisher, String.trim(name)}}
          ["UNSUB" | [topic]] -> {:ok, {:unsubscribe, String.trim(topic)}}
          ["UNSUB@" | [name]] -> {:ok, {:unsubscribe_publisher, String.trim(name)}}
          ["PUB" | [topic, message]] -> {:ok, {:publish, String.trim(topic), String.trim(message)}}
          ["ACK" | [ref]] -> {:ok, {:ack, String.trim(ref)}}
          ["PUBREL" | [ref]] -> {:ok, {:pubrel, String.trim(ref)}}
          ["PUBREC" | [ref]] -> {:ok, {:pubrec, String.trim(ref)}}
          ["PUBCOMP" | [ref]] -> {:ok, {:pubcomp, String.trim(ref)}}
          _ ->
            {:error, :unknown, "Command #{inspect command}."}
        end
    end
  end

  # ...
end
```

**Task 4** -- The message broker provides MQTT-style assured delivery;

As we can see in the previous task, the Command module provides us with PUBREL, PUBREC and PUBCOMP validation for commands. Firstly, in order to be able to identify a message or reply to its reference, we create an id in the MessageIDGenerator module. For demonstration purposes I commented out the hashing mechanism using sha256.

```elixir
defmodule MessageBroker.MessageIDGenerator do
  def generate_id(publisher, message) do
    message_id = publisher <> message
    IO.inspect(message_id)
    # :crypto.hash(:sha256, message_id)
    message_id |> Base.encode64
  end
end
```

We will see the Base64 for this message being put to use in a bit, for now let's see how the commands are being executed:

```elixir
defmodule MessageBroker.Command do
  # ...

  def run(client, {:publish, topic, message}) do
    if check_role(client, :producer) do
      if verify_message(client, topic, message) do
        status = MessageBroker.SubscriptionManager.publish(client, topic, message)

        case status do
          {:ok, ref} ->
            with {:pubrec, ref} <- MessageBroker.QoSManager.pub(:publisher, client, ref),
              do: {:ok, "PUBREC #{inspect ref}."}
          _ -> status
        end
      else
        {:error, :unsendable}
      end
    else
      {:error, :unauthorized, "publish"}
    end
  end

  def run(client, {:pubrel, ref}) do
    status = MessageBroker.QoSManager.pubrel(client, ref)

    case status do
      {:pubcomp, id} -> {:ok, "PUBCOMP #{inspect id}."}
      _ -> status
    end
  end

  def run(client, {:pubcomp, ref}) do
    status = MessageBroker.QoSManager.pubcomp(client, ref)

    case status do
      :ok -> MessageBroker.SubscriptionManager.ack_message(client, ref)
      _ -> status
    end
  end

  def run(client, {:pubrec, ref}) do
    status = MessageBroker.QoSManager.pubrec(client, ref)

    case status do
      {:pubrel, id} -> {:ok, "PUBREL #{inspect id}."}
      _ -> status
    end
  end

  # ...
end
```

We see that the QoSManager module is being used heavily for this logic:

```elixir
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
```

We actually have 3 maps:
- Publishers: In order to keep track the messages being sent from publishers, their name as well as tuples of the id of their message and the current state of the exchange is stored (:published, :pubrel).
- To-be-ack: Subscriber messages that have to be acknowledged with a PUBREC from them.
- To-be-notified: Subscriber messages that were acknowledged by all Subscribers and can be deleted.

They are being stored as files on hard disk and the module starts by loading them in the state.

QoSManager is also utilised in the Subscription Manager:

```elixir
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
```

where we could also see the use of SubscriberQueueManager that I explained the idea of previously (queue per topic = file per topic and sub message queues contain only the id).

```elixir
defmodule MessageBroker.SubscriberQueueManager do
  @queue_dir "messages/sub_queue"

  def add_message_to_queue(subscriber, topic, message_id) do
    dir = "#{@queue_dir}"
    path = "#{dir}/#{subscriber}"
    File.mkdir_p!(dir)
    existing_queue = case File.read(path) do
      {:ok, content} -> content <> "\n"
      {:error, :enoent} -> ""
    end
    new_message = "#{topic}: #{message_id}\n"
    File.write!(path, "#{existing_queue}#{new_message}")
  end

  def ack_message(subscriber, topic, message_id) do
    dir = "#{@queue_dir}"
    path = "#{dir}/#{subscriber}"
    {:ok, existing_queue} = File.read(path)
    acknowledged_message = "#{topic}: #{message_id}"
    lines = String.split(existing_queue, "\n")
    new_lines = Enum.filter(lines, fn line -> line != "" and String.trim(line) != acknowledged_message end)
    new_queue = Enum.join(new_lines, "\n")
    File.write!(path, new_queue)
    :ok
  end

  def get_topic_by_message_id(subscriber, message_id) do
    path = "#{@queue_dir}/#{subscriber}"
    case File.read(path) do
      {:ok, content} ->
        lines = String.split(content, "\n")
        line = Enum.find(lines, fn line -> String.contains?(line, message_id) end)
        if line do
          [topic | _] = String.split(line, ": ")
          {:ok, topic}
        else
          {:error, :ack, :no_message}
        end
      {:error, :enoent} -> {:error, :ack, :no_messages}
    end
  end

  def get_current_message(subscriber) do
    path = "#{@queue_dir}/#{subscriber}"
    case File.read(path) do
      {:ok, content} ->
        lines = String.split(content, "\n")
        case Enum.at(lines, 0) do
          nil -> {:error, :ack, :no_message}
          line ->
            split_line = String.split(line, ": ")
            case split_line do
              [topic, message_id] -> {:ok, {topic, message_id}}
              _ -> {:ok, "All done for now. Subscribe to more topics and have more messages!"}
            end
        end
      {:error, :enoent} -> {:error, :ack, :no_messages}
    end
  end
end
```

which is responsible for adding messages in a subscriber's queue or for the lookup.

**Task 5** -- The message broker ensures reliable message delivery using Persistent Messages with subscriber acknowledgments and Durable Queues;

So, far I mentioned storing state and messages a lot. Everything gets stored in different folders and files but the base path to keep the files separated is `/messages/folder_name`. Let's reiterate what exactly is getting stored:
- Dead Messages: path:"../dead" where a new file gets created every day and dead messages for that day are stored.
- QoSManager maps: path:"../qos" where there are three files for each map (publishers, to_be_ack and to_be_notified)
- Regular messages: path:"../regular" where files for every topic is created. Inside it message_id, pub_name, message_text is being stored with a new line.
- SubscriberQueueManager:  path:"../sub_queue" where a new file for each subscriber queue is created with the name as the sub_name. It stores the topic of the message and the message_id with a new line.
- Subscriptions: path:"../subscriptions/" where 2 files as 2 of the 4 maps Subscription Manager utilises are present:
  - topics
  - pub_sub
- Users: path:"../users/" where 2 files as the other 2 of the 4 maps Subscription Manager utilises are present:
  - publishers
  - subscribers

For this to happen there are two main modules, one for storing the state of other modules and another one for storing persisting the messages and message related information.

That is because in order to store the state (maps) I serialize the information as binary so that on load of the modules after a Message Broker restart, it wouldn't give any errors:

```elixir
defmodule MessageBroker.StateStore do
  @users_dir "messages/users"
  @subscriptions_dir "messages/subscriptions"
  @qos_dir "messages/qos" # New directory for QoS data

  def store_publishers(publishers) do
    store_map(@users_dir, "publishers", publishers)
  end

  def store_subscribers(subscribers) do
    store_map(@users_dir, "subscribers", subscribers)
  end

  def store_topics(topics) do
    store_map(@subscriptions_dir, "topics", topics)
  end

  def store_pub_sub(pub_sub) do
    store_map(@subscriptions_dir, "pub_sub", pub_sub)
  end

  def store_publishers_map(publishers) do
    store_map(@qos_dir, "publishers", publishers)
  end

  def store_to_be_ack_map(to_be_ack) do
    store_map(@qos_dir, "to_be_ack", to_be_ack)
  end

  def store_to_be_notified_map(to_be_notified) do
    store_map(@qos_dir, "to_be_notified", to_be_notified)
  end

  def load_publishers_map() do
    load_map(@qos_dir, "publishers")
  end

  def load_to_be_ack_map() do
    load_map(@qos_dir, "to_be_ack")
  end

  def load_to_be_notified_map() do
    load_map(@qos_dir, "to_be_notified")
  end

  def load_publishers() do
    load_map(@users_dir, "publishers")
  end

  def load_subscribers() do
    load_map(@users_dir, "subscribers")
  end

  def load_topics() do
    load_map(@subscriptions_dir, "topics")
  end

  def load_pub_sub() do
    load_map(@subscriptions_dir, "pub_sub")
  end

  defp store_map(dir, filename, map) do
    path = "#{dir}/#{filename}"
    File.mkdir_p!(dir)
    binary_map = :erlang.term_to_binary(map)
    File.write!(path, binary_map)
  end

  defp load_map(dir, filename) do
    path = "#{dir}/#{filename}"
    case File.read(path) do
      {:ok, binary_content} ->
        parsed_map = :erlang.binary_to_term(binary_content)
        parsed_map
      {:error, :enoent} ->
        %{}
    end
  end
end
```

As well as the message (dead/regular) persisting module called MessageStore:

```elixir
defmodule MessageBroker.MessageStore do
  @base_path "messages"
  @messages_dir "messages/regular"

  def store_message(client, message, type) do
    case type do
      :dead -> store_dead_message("Client #{inspect client} attempted: #{inspect message}")
      _ -> {:error, "Unknown message type"}
    end
  end

  def get_message(topic, message_id) do
    path = "#{@messages_dir}/#{topic}"
    case File.read(path) do
      {:ok, content} ->
        content
        |> String.split("\n")
        |> Enum.find(fn line -> String.starts_with?(line, message_id) end)
        |> case do
          nil -> {:error, :ack, :message_not_found}
          line ->
            [_, publisher, message] = String.split(line, ~r/\s+/, parts: 3)
            {:ok, {publisher, message}}
        end

      {:error, :enoent} -> {:error, :ack, :topic_not_found}
    end
  end

  def store_regular_message(topic, message, publisher, message_id) do
    dir = "#{@messages_dir}"
    path = "#{dir}/#{topic}"
    File.mkdir_p!(dir)
    existing_messages = case File.read(path) do
      {:ok, content} -> content <> "\n"
      {:error, :enoent} -> ""
    end
    File.write!(path, "#{existing_messages}#{message_id} #{publisher}: #{inspect message}")
  end

  defp store_dead_message(message) do
    path = Path.join(@base_path, "dead")
    File.mkdir_p!(path)

    timestamp = DateTime.to_unix(DateTime.utc_now())
    filename = "#{timestamp}_dead_message.txt"
    filepath = Path.join(path, filename)

    message_string = format_message(message)
    File.write!(filepath, message_string)
    {:ok, filepath}
  end

  defp format_message(message) do
    # Format the message as a string, depending on your message structure.
    # For example, if the message is a tuple, you can use inspect/1:
    # inspect(message)
    message
  end
end
```

This way even after reconnecting, there will be no information regarding what publishers/subscribers registered, what topics and messages are adressed to what subscriber and which of the messages have already been acknowledged by the users using QoS 2 even if the Message Broker goes down or a user disconnects.

**Task 6** -- The project provides a Docker container to run the message broker.

As I already mentioned, in order for the docker-compose.yml file to work correctly, I included a Dockerfile for the message broker which is the following:

```Dockerfile
# Dockerfile
FROM elixir:1.12.3-alpine

WORKDIR /app

RUN mix local.hex --force && \
    mix local.rebar --force

# Install dependencies
COPY mix.exs mix.lock ./
RUN mix do deps.get, deps.compile

# Copy all application files
COPY . .

EXPOSE 4040

CMD ["mix", "run", "--no-halt"]
```

It loads the dependencies and runs the message broker on the default port 4040 thus exposing exactly this port.

The following is the .dockerignore:
```dockerignore
# .dockerignore
/_build
/deps
/priv
/test
```

## Bonus

**Task 1** -- The project allows for using the application from Project 1 as a producer;

In order to implement the task, the only changes occured to the project 1 in fact. The Message Broker allowed for using any application as a producer as long as they went through the registering steps correctly.

After the batch of tweets is received in the Batcher module (that is, the tweets are correctly agregated) the only differences are that now a new module is called besides storing the message in the database. The following lines are the addition:

```elixir
IO.puts("============Batch of #{length(acc)} tweets is ready.=================")

Enum.each(acc, fn %{tweet: text, sentiment: sentiment, engagement: engagement, username: username} ->
  bpid = BrokerSupervisor.get_broker(username)
  Broker.twt(bpid, text)
end)
```

I added a Broker worker which actually represents a Producer, as well as a BrokerSupervisor which would create these workers.

The idea is that a Broker is identified by a username. In case the Publisher was already registered with this name, we simply get the pid of the worker by finding it by the username using the DynamicSupervisor. In case it is not, the Supervisor creates the worker and then provides the pid. After that the worker is simply called with the text of the tweet since the name of the user is already registered from the Broker module.

This is the code for the Supervisor:
```elixir
defmodule BrokerSupervisor do
  use DynamicSupervisor

  def start_link() do
    DynamicSupervisor.start_link(__MODULE__, nil, name: __MODULE__)
  end

  def init(_init_args) do
    DynamicSupervisor.init(strategy: :one_for_one)
  end

  def start_broker(username) do
    {:ok, socket} = :gen_tcp.connect('localhost', 4040, [:binary, packet: :line, active: false])
    read(socket)
    push(socket, "PUB")
    read(socket)
    push(socket, username)
    read(socket)
    read(socket)
    push(socket, "tcp")
    read(socket)
    {:ok, pid} = DynamicSupervisor.start_child(__MODULE__, {Broker, {username, socket}})
    {:ok, pid}
  end

  def get_broker(username) do
    children = DynamicSupervisor.which_children(__MODULE__)
    broker = Enum.find(children, fn {id, _, _, _} -> id == username end)
    case broker do
      nil ->
        {:ok, pid} = start_broker(username)
        pid
      {_, pid, _, _} -> pid
    end
  end

  defp push(socket, line) do
    :ok = send_line(socket, line)
    IO.puts("Sent: #{line}")
  end

  defp read(socket) do
    {:ok, response} = receive_line(socket)
    IO.puts("Received response: #{response}")
  end

  defp send_line(socket, line) do
    :gen_tcp.send(socket, "#{line}\r\n")
  end

  defp receive_line(socket) do
    :gen_tcp.recv(socket, 0)
  end
end
```

In the start_broker(username) function are included the registering steps of a new producer:
- Selecting PUB as the role
- Inputting the name as the username
- Selecting tcp as the protocol by default

And the following is the code of the worker:
```elixir
defmodule Broker do
  use GenServer

  def start_link({username, socket}) do
    GenServer.start_link(__MODULE__, {username, socket})
  end

  def init({username, socket}) do
    {:ok, {username, socket}}
  end

  def twt(pid, text) do
    GenServer.call(pid, {:twt, text})
  end

  def handle_call({:twt, text}, _from, {username, socket}) do
    push(socket, "PUB|#{username}|#{text}")
    push(socket, "END")
    read(socket)

    message_id = username <> text
    ref =  Base.encode64(message_id)

    push(socket, "PUBREL|#{ref}")
    push(socket, "END")
    read(socket)

    {:reply, :ok, {username, socket}}
  end

  defp push(socket, line) do
    :ok = send_line(socket, line)
    IO.puts("Sent: #{line}")
  end

  defp read(socket) do
    {:ok, response} = receive_line(socket)
    IO.puts("Received response: #{response}")
  end

  defp send_line(socket, line) do
    :gen_tcp.send(socket, "#{line}\r\n")
  end

  defp receive_line(socket) do
    :gen_tcp.recv(socket, 0)
  end
end
```

While being called witht the twt(pid, text) function, the worker uses the:
```bash
PUB|topic|text
```
command to publish the message.

After which it also replies with the:
```bash
PUBREL|ref
```
command on PUBREC|ref from the Message Broker to acknowledge the message was sended succesfully.

**Task 2** -- The message broker implements a “polyglot” API. The client can choose between UDP /TCP or MQTT / XMPP / AMQP, with protocol negotiation similar to HTTP’s content negotiation;

In order to implement this functionality, I have added a polyglot module that would handle this decision and also added the following lines in the Authenticator when choosing the role and name to also be prompted to choose between tcp and mqtt:

```elixir
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
```

```elixir
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
    GenServer.call(:polyglot, {:store_protocol, socket, protocol})
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
```

In order to handle the communication for each of the options I have two different communication handlers, one for MQTT:

```elixir
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
```
which utilises the Tortoise library for MQTT communication.

As well as the TCP communication handling:
```elixir
defmodule MessageBroker.TerminalHandler do
  use GenServer

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: :terminal_handler)
  end

  def init(_opts) do
    {:ok, %{}}
  end

  def read_line(socket) do
    :gen_tcp.recv(socket, 0)
  end

  defp send_client(socket, text) do
    :gen_tcp.send(socket, "#{text}\r\n")
  end

  def write_line(socket, {:ok, text}) do
    send_client(socket, text)
  end

  def write_line(socket, {:error, :ack, reason}) do
    case reason do
      :no_message -> send_client(socket, "No such message was addressed to you. Please check your subscriptions.")
      :message_not_found -> send_client(socket, "A message with such ID was not found. Please check your spelling.")
      :topic_not_found ->
        send_client(socket, "No such topic was found. Erasing message...")
        MessageBroker.SubscriptionManager.next_message(socket)
        send_client(socket, "Corrupted message erased.")
      :no_messages -> send_client(socket, "You don't have any messages. Consider subscribing to more topics!")
      _ -> write_line(socket, {:error, reason})
    end
  end

  def write_line(socket, {:error, :qos, reason}) do
    case reason do
      :unknown_role -> send_client(socket, "Unknown Role. Please register again.")
      :no_pub -> send_client(socket, "You don't have such a message to PUBREL to.")
      :no_sub -> send_client(socket, "You don't have such a message to PUBREC to.")
      :no_comp -> send_client(socket, "You don't have such a message to PUBCOMP to.")
      :topic_not_found ->
        send_client(socket, "No such topic was found. Erasing message...")
        MessageBroker.SubscriptionManager.next_message(socket)
        send_client(socket, "Corrupted message erased.")
      :no_messages -> send_client(socket, "You don't have any messages. Consider subscribing to more topics!")
      _ -> write_line(socket, {:error, reason})
    end
  end

  # Add these new lines to the write_line/2 function
  def write_line(socket, {:ok, :pubrec, ref}) do
    send_client(socket, "PUBREC|#{ref}")
  end

  def write_line(socket, {:ok, :pubrel, ref}) do
    send_client(socket, "PUBREL|#{ref}")
  end

  def write_line(socket, {:ok, :pubcomp, ref}) do
    send_client(socket, "PUBCOMP|#{ref}")
  end

  def write_line(socket, {:error, :invalid_pubrec}) do
    send_client(socket, "Invalid PUBREC")
  end

  def write_line(socket, {:error, :invalid_pubrel}) do
    send_client(socket, "Invalid PUBREL")
  end

  def write_line(socket, {:error, :unknown, reason}) do
    # Known error; write to the client
    send_client(socket, "Unknown #{reason}")
  end

  def write_line(socket, {:error, :unauthorized, action}) do
    send_client(socket, "Unauthorized: As a #{MessageBroker.RoleManager.get_readable_role(socket)} you don't have permission to #{action}.")
  end

  def write_line(socket, {:error, :sub_manager, reason}) do
    case reason do
      :already_subscribed -> send_client(socket, "Already subscribed to the topic.")
      :not_subscribed -> send_client(socket, "In order to unsubscribe you have to first subscribe to the topic.")
      :not_subscribed_publisher -> send_client(socket, "In order to unsubscribe you have to first subscribe to this publisher.")
      :publisher_not_found -> send_client(socket, "No such Publisher found. Please check your spelling.")
      :already_subscribed_to_publisher -> send_client(socket, "Already subscribed to this Publisher.")
      _ -> write_line(socket, {:error, reason})
    end
  end

  def write_line(_socket, {:error, :closed}) do
    # The connection was closed, exit politely
    exit(:shutdown)
  end

  def write_line(socket, {:error, :unsendable}) do
    # Message was dead
    send_client(socket, "Cannot load the tweet.")
    # exit(error)
  end

  def write_line(socket, {:error, error}) do
    # Unknown error; write to the client and exit
    send_client(socket, "Error #{inspect error}")
    exit(error)
  end
end
```

**Task 3** -- The project implements Change Data Capture, thus making the publisher the database from Project 1 rather than a dedicated application;

I avoided using a database such as Postrgre or MySQL and instead used a simple file with a record for each newline. The records are stored with the form of `pub_name: message`. I created the database records using Project 1, instead of simply storing internally the records of the database I write them in a file:

```elixir
defmodule Database do

  # ...

  def store_on_disk(username, tweet) do
    dir = "database"
    path = "#{dir}/records"
    File.mkdir_p!(dir)
    existing_messages = case File.read(path) do
      {:ok, content} -> content <> "\n"
      {:error, :enoent} -> ""
    end
    File.write!(path, "#{existing_messages}#{username}: #{tweet}")
  end

  # ...

end
```

Then, I am reading this file in my CDC module:

```elixir
defmodule MessageBroker.CDC do
  use GenServer

  def start_link(file) do
    GenServer.start_link(__MODULE__, file)
  end

  def init(file) do
    dir_path = Path.dirname(file)
    IO.inspect("Watching directory: #{dir_path}")
    {:ok, watcher} = FileSystem.start_link(dirs: [dir_path])
    FileSystem.subscribe(watcher)
    IO.inspect("FileSystem watcher pid: #{inspect(watcher)}")
    {:ok, {file, watcher}}
  end

  def handle_info(msg, state) do
    IO.inspect("Received message: #{inspect(msg)}")
    {file, _pid} = state

    case msg do
      {:file_event, _watcher, {path, _events}} when path == file ->
        File.stream!(file)
        |> Stream.each(&publish_change/1)
        |> Stream.run()

        {:noreply, state}

      _ ->
        {:noreply, state}
    end
  end

  defp publish_change(line) do
    {:ok, message} = MessageBroker.SubscriptionManager.publish(line)
    IO.inspect(message)
  end
end
```

In order to listen to changes of the 'database' file I used the FyleSystem which allows the use of a watcher in order to notice any changes to the file. In order to publish these messages from the database, there exists another publish method in the SubscriptionManager that doesn't require a socker (we are not using a dedicated application, but a database that doesn't require a connection) and the replies (message_id and the message text) are saved in a new file inside `messages/regular/db_change`.

```elixir
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
```

When a change to the file has happened, a new record in the `db_change` file is added after verifying the validity of the change. The record is the message's id generated in Base64, the name of the publisher as well as it's text.

**Task 4** -- The project provides a docker-compose file to start all applications at once

I have a docker-compose.yml file to satisfy this need, but first we have a look at the Dockerfile of the Message Broker:

```Dockerfile
# Dockerfile
FROM elixir:1.12.3-alpine

WORKDIR /app

RUN mix local.hex --force && \
    mix local.rebar --force

# Install dependencies
COPY mix.exs mix.lock ./
RUN mix do deps.get, deps.compile

# Copy all application files
COPY . .

EXPOSE 4040

CMD ["mix", "run", "--no-halt"]
```

It loads the dependencies and runs the message broker on the default port 4040 thus exposing exactly this port.

The following is the .dockerignore:
```dockerignore
# .dockerignore
/_build
/deps
/priv
/test
```

Moreover, Project 1 also requires a pretty straightforward Dockerfile quite similar to the Message Broker's:
```Dockerfile
# Dockerfile
FROM elixir:1.12.3-alpine

WORKDIR /app

RUN mix local.hex --force && \
    mix local.rebar --force

# Install dependencies
COPY mix.exs mix.lock ./
RUN mix do deps.get, deps.compile

# Copy all application files
COPY . .

CMD ["mix", "run", "--no-halt"]
```
However, not exposing a certain PORT as Project 1 doesn't require it. The Publishers are connecting using telnet. 
It has the exact same .dockerignore file.

The docker-compose.yml file is the following:
```yaml
version: '3'
services:
  message_broker:
    build:
      context: ./lab_2/message_broker
      dockerfile: Dockerfile
    ports:
      - 4040:4040

  my_app:
    build:
      context: ./lab 1
      dockerfile: Dockerfile
    depends_on:
      - message_broker
      - database_container

  database_container:
    image: alexburlacu/rtp-server:faf18x
    ports:
      - 4000:4000
```
I provide the path to the Dockerfile for both applications, as well as the image of the database Project 1 depends on, exposing PORT 4000 for it.

We can run this with one single command:
```bash
docker-compose up --build
```

## Conclusion

In conclusion, the Message Broker project has provided invaluable insights into crucial aspects of message brokering, delivering messages efficiently, and facilitating seamless communication between producers and consumers. Inspired by Kafka's concepts of topics and queues, the project demonstrates the significance of organizing messages based on subscribers' interests, ultimately showcasing their practical application in real-world scenarios.

Moreover, the project underscores the criticality of reliable message delivery through the implementation of MQTT-style assured delivery, Quality of Service (QoS) 2, and the utilization of persistent messages with subscriber acknowledgments and durable queues. By incorporating essential modules like the Subscription Manager and QoS Manager, while also ensuring message state persistence, the project highlights the utmost importance of robustness and data integrity in message broker systems.

Additionally, the project impressively integrates with existing applications, as exemplified by the successful incorporation of Project 1. This showcases the exceptional flexibility and extensibility of message broker architectures, effectively enabling the system to handle various message types, including tweets, and significantly expanding the possibilities for seamless application integration.

Furthermore, the project recognizes the value of protocol flexibility by offering a versatile "polyglot" API, accommodating clients' preferred communication protocols such as TCP or MQTT. The implementation of a protocol negotiation mechanism further emphasizes the project's commitment to facilitating smooth communication between diverse systems.

Overall, the Message Broker project provides a comprehensive learning experience, covering key topics in message brokering, efficient message delivery, integration with existing applications, and protocol flexibility. It effectively demonstrates the practical application of these concepts and technologies, thereby laying the groundwork for the development of highly efficient and robust communication systems.

## Bibliography

1. [MQTT and QoS 2](https://emqx.medium.com/introduction-to-mqtt-5-0-protocol-qos-quality-of-service-e6d9b0aaf9fb)
2. [TCP and :gen_tcp](https://elixir-lang.org/getting-started/mix-otp/task-and-gen-tcp.html)
3. [Durable Queues](https://medium.com/@msrijita189/understanding-kafka-durability-and-availability-a832c5535678)
4. [Change Data Capture](https://www.qlik.com/us/change-data-capture/cdc-change-data-capture)
7. [Dynamic Supervisor](https://hexdocs.pm/elixir/1.13/DynamicSupervisor.html)