defmodule MessageBroker.Database do
  use GenServer

  def start_link(file) do
    GenServer.start_link(__MODULE__, file)
  end

  def init(file) do
    dir_path = Path.dirname(file)
    IO.inspect("Watching directory: #{dir_path}")
    {:ok, watcher} = FileSystem.start_link(dirs: [dir_path])
    FileSystem.subscribe(watcher)
    IO.inspect("FileSystem watcher pid: #{inspect(watcher)}")
    {:ok, {file, watcher}}
  end

  def handle_info(msg, state) do
    IO.inspect("Received message: #{inspect(msg)}")
    {file, _pid} = state

    case msg do
      {:file_event, _watcher, {path, _events}} when path == file ->
        File.stream!(file)
        |> Stream.each(&publish_change/1)
        |> Stream.run()

        {:noreply, state}

      _ ->
        {:noreply, state}
    end
  end

  defp publish_change(line) do
    {:ok, message} = MessageBroker.SubscriptionManager.publish(line)
    IO.inspect(message)
  end
end
