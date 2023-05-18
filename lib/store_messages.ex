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
