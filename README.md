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

### It should also be possible to run directly as a streamlit app, 
- It would be best to be in a anaconda/virtual python environment.  
- Install the packages based on the Dockerfile PIP commands, and it should be good to go.

# What SnowFlurry is/does:
 - Snowflurry is designed to help size a (multicluster) warehouse for a workload.  It allows the user to set various parameters, runs sample workloads, and collects the results.
 -  For example, a warehouse that will be used to support a multiple user dashboard:  There will be a number of concurrent, similar queries sent to the warehouse, and using the ideal size and MCW count will ensure adequate response times for the users.

# NOTE:  During testing the warehouse selected will be resized/stopped/started.  It should only be used by the SnowFlurry testing, to ensure accurate results.  Other work running against it could prevent/delay resizing and scaling.
## The User must have permission to execute all the queries in the SQL file, resize/start/stop the warehouse.

## Inputs
- Iterations:  How many queries will be sent to the database at one time (asyncronously).  This will essentially use the SQL file multiple times, to create enough queries.

- Sql File: Create a SQL file with a representative sample of the types of queries that will be executing on the warehouse.  The more actual use cases covered, the better.  The number of queries does not matter, as the iterations parameter will allow the file yo but used multiple times, to simulate many users.

- Warehouse size: Start small, execute your script with a given (small) iterations.  The results (recommend you copy/paste into excel to collect the various runs) wiil include information at the cluster level (recommend starting at 1:1), and totals.  The statistics collected are min/max/sum/avg of:  Rows returned, bytes scanned, compile time, queue time, execution time, and elapsed (combined of the others) times.

Increasing only the warehouse size, look at the impact to execution time.  Each increase should roughly half the execution time, if the queries are making full use of the resources of the larger warehouse.  At some point, larger warehouses will have diminishing returns.  The warehouse size below that is probably optimal.

- MCW Size: Set the Iterations count large enough to make use of multiple warehouses (but maybe not all the way to desire concurrency level yet) and run with additional MCW instances.  This should have the impact of reducing queue time (as well as elapsed time).  The average execution time should not be impacted, but with more instances, more concurrent jobs will be processed.

Continue adjusting the MCW/Iterations until you reach a reasonable balance of price/performance.

The sizing exercise is impacted by the queries used in the SQL file, so consider tuning them ahead of this exercise, and revisiting this exercise if significant changes are made.

## Future enhancement ideas:
- Save the configuration changes (Save Button)
- Add button to cancel execution.  Abort all queries, suspend warehouse
