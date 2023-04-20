defmodule MessageBroker.Command do
  @doc ~S"""
  Parses the given `line` into a command.
  """

  def parse(line) do
    data = String.trim(line) # get rid of \r\n
    parts = String.split(data, "|")

    case parts do
      ["SUB" | [topic]] -> {:ok, {:subscribe_topic, String.trim(topic)}}
      ["SUB@" | [name]] -> {:ok, {:subscribe_publisher, String.trim(name)}}
      ["UNSUB" | [topic]] -> {:ok, {:unsubscribe, String.trim(topic)}}
      ["UNSUB@" | [name]] -> {:ok, {:unsubscribe_publisher, String.trim(name)}}
      ["PUB" | [topic, message]] -> {:ok, {:publish, String.trim(topic), String.trim(message)}}
      _ -> {:error, :unknown, "command #{inspect data}."}
    end
  end

  def run(client, {:subscribe_topic, topic}) do
    if MessageBroker.RoleManager.check_role(client, :consumer) do
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
    if MessageBroker.RoleManager.check_role(client, :consumer) do
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
    if MessageBroker.RoleManager.check_role(client, :consumer) do
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
    if MessageBroker.RoleManager.check_role(client, :consumer) do
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
    if MessageBroker.RoleManager.check_role(client, :producer) do
      status = MessageBroker.SubscriptionManager.publish(client, topic, message)

      case status do
        :ok -> {:ok, "Published message #{inspect message} to topic: #{inspect topic}."}
        _ -> status
      end
    else
      {:error, :unauthorized, "publish"}
    end
  end
end
