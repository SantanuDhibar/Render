FROM denoland/deno:latest

WORKDIR /app

COPY . .

RUN deno cache server.ts

EXPOSE 8080

CMD ["deno", "run", "--allow-net", "--allow-env", "--allow-read", "--allow-run", "server.ts"]
