defmodule MessageBroker.Client do
  def serve(socket) do
    msg =
      with {:ok, data} <- MessageBroker.TerminalHandler.read_line(socket),
      {:ok, command} <- MessageBroker.Command.parse(data),
      do: MessageBroker.Command.run(socket, command)

    MessageBroker.TerminalHandler.write_line(socket, msg)
    serve(socket)
  end
end
