defmodule MessageBroker.Command do
  @doc ~S"""
  Parses the given `line` into a command.
  """

  def parse(line) do
    data = String.trim(line) # get rid of \r\n
    parts = String.split(data, "|")

    case parts do
      ["SUBSCRIBE" | [topic]] -> {:ok, {:subscribe, String.trim(topic)}}
      ["UNSUBSCRIBE" | [topic]] -> {:ok, {:unsubscribe, String.trim(topic)}}
      ["PUBLISH" | [topic, message]] -> {:ok, {:publish, String.trim(topic), String.trim(message)}}
      _ -> {:error, :unknown, "command #{inspect data}."}
    end
  end

  def run(client, {:subscribe, topic}) do
    if MessageBroker.RoleManager.check_role(client, :consumer) do
      status = MessageBroker.SubscriptionManager.subscribe(client, topic)

      case status do
        :ok -> {:ok, "Subscribed to topic: #{inspect topic}."}
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

  def run(client, {:publish, topic, message}) do
    if MessageBroker.RoleManager.check_role(client, :producer) do
      status = MessageBroker.SubscriptionManager.publish(client, topic, message)

      case status do
        :ok -> {:ok, "Published message #{inspect message} to #{inspect topic}."}
        _ -> status
      end
    else
      {:error, :unauthorized, "publish"}
    end
  end
end
