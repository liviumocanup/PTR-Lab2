defmodule MessageBroker.MessageIDGenerator do
  def generate_id(publisher, message) do
    message_id = publisher <> message
    IO.inspect(message_id)
    # :crypto.hash(:sha256, message_id)
    message_id |> Base.encode64
  end
end
