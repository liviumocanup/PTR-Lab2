defmodule MessageBroker.Client do
  def serve(socket, buffer \\ "") do
    {:ok, data} = MessageBroker.TerminalHandler.read_line(socket)
    command = buffer <> data

    cond do
      String.contains?(command, "END\r\n") ->
        # complete command received, parse it
        msg =
          with {:ok, cmd} <- MessageBroker.Command.parse(command),
            do: MessageBroker.Command.run(socket, cmd)

        MessageBroker.TerminalHandler.write_line(socket, msg)
        serve(socket)

      true ->
        # incomplete command, continue buffering
        serve(socket, command)
    end
  end
end
