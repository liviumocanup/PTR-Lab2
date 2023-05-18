# Dockerfile
FROM elixir:1.12.3-alpine

WORKDIR /app

RUN mix local.hex --force && \
    mix local.rebar --force

# Install dependencies
COPY mix.exs mix.lock ./
RUN mix do deps.get, deps.compile

# Copy all application files
COPY . .

EXPOSE 4040

CMD ["mix", "run", "--no-halt"]
