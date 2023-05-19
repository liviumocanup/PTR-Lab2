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
