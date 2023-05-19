defmodule MessageBroker.Application do
  use Application

  def start(_type, _args) do
    port = String.to_integer(System.get_env("PORT") || "4040")
    file = "c:/Personal/uni/lessons/ptr/lab_2/message_broker/db/records"

    children = [
      MessageBroker.TerminalHandler,
      MessageBroker.Auth,
      MessageBroker.RoleManager,
      MessageBroker.SubscriptionManager,
      MessageBroker.QoSManager,
      MessageBroker.Polyglot,
      {MessageBroker.Database, file},
      {Task.Supervisor, name: MessageBroker.TaskSupervisor},
      Supervisor.child_spec({Task, fn -> MessageBroker.Server.accept(port) end}, restart: :permanent),
    ]

    opts = [strategy: :one_for_one, name: MessageBroker.Supervisor]
    Supervisor.start_link(children, opts)
  end
end
