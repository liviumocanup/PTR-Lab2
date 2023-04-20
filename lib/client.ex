defmodule MessageBroker.Client do
  def serve(socket) do
    # input = read_line(socket)

    msg =
      with {:ok, data} <- read_line(socket),
      {:ok, command} <- MessageBroker.Command.parse(data),
      do: MessageBroker.Command.run(socket, command)

    write_line(socket, msg)
    serve(socket)
  end

  defp read_line(socket) do
    :gen_tcp.recv(socket, 0)
  end

  defp write_line(socket, {:ok, text}) do
    send_client(socket, text)
  end

  defp write_line(socket, {:error, :unknown, reason}) do
    # Known error; write to the client
    send_client(socket, "Unknown #{reason}")
  end

  defp write_line(socket, {:error, :unauthorized, action}) do
    send_client(socket, "Unauthorized: As a #{MessageBroker.RoleManager.get_readable_role(socket)} you don't have permission to #{action}.")
  end

  defp write_line(socket, {:error, :sub_manager, reason}) do
    case reason do
      :already_subscribed -> send_client(socket, "Already subscribed to the topic.")
      :not_subscribed -> send_client(socket, "In order to unsubscribe you have to first subscribe to the topic.")
      _ -> write_line(:error, reason)
    end
  end

  defp write_line(_socket, {:error, :closed}) do
    # The connection was closed, exit politely
    exit(:shutdown)
  end

  defp write_line(socket, {:error, error}) do
    # Unknown error; write to the client and exit
    send_client(socket, "Error #{inspect error}")
    exit(error)
  end

  defp send_client(socket, text) do
    :gen_tcp.send(socket, "#{text}\r\n")
  end

  def assign_role(socket) do
    if MessageBroker.RoleManager.has_role?(socket) == false do
      write_line(socket, {:ok, "Do you wish to be a Publisher or Subscriber? PUB/SUB"})

      msg =
        with {:ok, data} <- read_line(socket),
        :ok <- MessageBroker.RoleManager.check_and_assign(socket, String.trim(data)),
        do: {:ok, "Successfully assigned role."}

      write_line(socket, msg)
      case msg do
        {:error, :unknown, _} -> assign_role(socket)
        {:ok, _} -> serve(socket)
      end
    end
  end

  # def assign_role(socket) do
  #   if MessageBroker.RoleManager.has_role?(socket) == false do
  #     write_line(socket, {:ok, "Do you wish to be a Publisher or Subscriber? PUB/SUB"})

  #     msg =
  #       with {:ok, data} <- read_line(socket),
  #       role = String.trim(data),
  #       :ok <- handle_role_input(socket, role),
  #       do: {:ok, "Successfully assigned role."}

  #     write_line(socket, msg)
  #     case msg do
  #       {:error, :unknown, _} -> assign_role(socket)
  #       {:ok, _} -> serve(socket)
  #     end
  #   end
  # end

  # defp handle_role_input(socket, "PUB") do
  #   write_line(socket, {:ok, "Please enter a publisher name:"})
  #   with {:ok, data} <- read_line(socket),
  #     name = String.trim(data),
  #     do: MessageBroker.RoleManager.check_and_assign(socket, "PUB", name)
  # end

  # defp handle_role_input(socket, "SUB") do
  #   MessageBroker.RoleManager.check_and_assign(socket, "SUB", nil)
  # end

  # defp handle_role_input(_, _) do
  #   {:error, :unknown, "role. Please enter 'PUB' or 'SUB'."}
  # end

end
