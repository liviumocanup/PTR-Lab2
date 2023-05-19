defmodule MessageBroker.DeadLetterChannel do
  def check_message(client, topic, message) do
    if message != "" do
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
