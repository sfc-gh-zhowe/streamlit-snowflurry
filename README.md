# SnowFlurry Streamlit Environment

This repository contains steps to set up a container image containing [Streamlit](https://streamlit.io/) server with `snowflake-snowpark-python` and `pandas` packages

## One-time setup
- make sure you have installed either docker or [podman](https://podman.io/).
    - Also needed are `docker compose` (included in the newer versions of docker) or `podman-compose`
    - If you use *podman*, substitute *docker* references with *podman*. For example, use `podman-compose` if the instructions call for running `docker compose`
- clone this repo to a new directory and `cd` to that directory
- Update `snowflurry.json` file in `./secrets` folder with connection information, or fill it in dynamically at run-time.
- currently key-pair authentication is not supported....

## Starting the container
- Run `docker compose up -d` to start the container which in turn will start the `streamlit` server
- Run `docker compose logs` to print the connection URL containing the **token**
- Click, or copy-paste, the URL to start a streamlit session

## Stopping the container
- Run `docker compose down`
