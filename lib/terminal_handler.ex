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
