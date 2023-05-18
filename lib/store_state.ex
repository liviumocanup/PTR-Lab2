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
