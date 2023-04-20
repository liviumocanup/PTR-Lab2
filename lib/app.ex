defmodule MessageBroker.Application do
  use Application

  def start(_type, _args) do
    port = String.to_integer(System.get_env("PORT") || "4040")

    children = [
      MessageBroker.TerminalHandler,
      MessageBroker.Auth,
      MessageBroker.RoleManager,
      MessageBroker.SubscriptionManager,
      {Task.Supervisor, name: MessageBroker.TaskSupervisor},
      Supervisor.child_spec({Task, fn -> MessageBroker.Server.accept(port) end}, restart: :permanent)
    ]

    opts = [strategy: :one_for_one, name: MessageBroker.Supervisor]
    Supervisor.start_link(children, opts)
  end
end
