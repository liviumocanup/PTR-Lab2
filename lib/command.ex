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
