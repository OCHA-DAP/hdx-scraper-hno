# Summary


# Information

This library is part of the
[Humanitarian Data Exchange](https://data.humdata.org/) (HDX) project. If you
have humanitarian related data, please upload your datasets to HDX.

The code for the library is
[here](https://github.com/OCHA-DAP/hdx-data-freshness). The library has
detailed API documentation which can be found in the menu at the top.


# Description


## Docker Setup

The Dockerfile installs required packages and also the dependencies listed in
`requirements.txt`. It uses the Python source files directly (rather than
using PyPI as was the case previously).

# Usage

In the command line usage examples below, common parameters are set as follows:

Either db_uri or db_params must be provided or the environment variable DB_URI
must be set. db_uri or DB_URI are of form:
`postgresql+psycopg://user:password@host:port/database`

db_params is of form:
`database=XXX,host=X.X.X.X,username=XXX,password=XXX,port=1234,
ssh_host=X.X.X.X,ssh_port=1234,ssh_username=XXX,
ssh_private_key=/home/XXX/.ssh/keyfile`

## Command line

    python -m hdx.hapi.app PARAMETERS

The PARAMETERS are:

    -hk HDX_KEY, --hdx_key HDX_KEY
                        HDX api key
    -ua USER_AGENT, --user_agent USER_AGENT
                        user agent
    -hs HDX_SITE, --hdx_site HDX_SITE
                        HDX site to use
    -db DB_URI, --db_uri DB_URI
                        Database connection string
    -dp DB_PARAMS, --db_params DB_PARAMS
                        Database connection parameters. Overrides --db_uri.
    -s, --save          Save state for testing
