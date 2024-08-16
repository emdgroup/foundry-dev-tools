FROM --platform=linux/arm64 ghcr.io/prefix-dev/pixi:0.27.1 AS build

COPY pixi.lock /app/pixi.lock
COPY pixi.toml /app/pixi.toml
WORKDIR /app
RUN pixi install --environment lambda --frozen

FROM --platform=linux/arm64 ubuntu:22.04 AS production

# only copy the production environment into prod container
COPY --from=build /app/.pixi/envs/lambda /app/.pixi/envs/lambda
# Copy sources
COPY lambda_src.py /app/lambda_src.py
WORKDIR /app

# Set runtime interface client as default command for the container runtime
ENTRYPOINT [ "/app/.pixi/envs/lambda/bin/python", "-m", "awslambdaric" ]
# Pass the name of the function handler as an argument to the runtime
CMD [ "lambda_src.lambda_handler" ]
