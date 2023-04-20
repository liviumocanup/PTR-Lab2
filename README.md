# Real Time Programming Laboratory Work 2

### University: _Technical University of Moldova_
### Faculty: _Computers, Informatics and Microelectronics_
### Department: _Software Engineering and Automatics_
### Author: _Mocanu Liviu_
### Verified by: _asist. univ. Alexandru Osadcenco_

----

## Abstract
&ensp;&ensp;&ensp; This repository contains the laboratory work 2 tasks on the PTR subject at TUM.

## Description
The project is an implementation of Message Broker.
```bash
$ telnet 127.0.0.1 PORT
```
where PORT is the port the Message Broker operates on (the default is 4040).

-Users open a telnet terminal and decide whether they want to be Publishers or Subscribers.
-Users are asked to input SUB or PUB when first connecting.
-Publishers can only publish a message on a certain topic 
-Subscribers can only subscribe and unsubscribe from a topic. 

## Run

To run the program, execute the following commands.
```bash
PORT=4321 mix run --no-halt
```
for starting the Message Broker on port 4321 for example.

## Installation

If [available in Hex](https://hex.pm/docs/publish), the package can be installed
by adding `message_broker` to your list of dependencies in `mix.exs`:

```elixir
def deps do
  [
    {:message_broker, "~> 0.1.0"}
  ]
end
```

Documentation can be generated with [ExDoc](https://github.com/elixir-lang/ex_doc)
and published on [HexDocs](https://hexdocs.pm). Once published, the docs can
be found at <https://hexdocs.pm/message_broker>.

