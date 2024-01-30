# template-python-docker
A template repository for getting a project up and running with docker and python

## Setup:

1. Create a new repository using this as the template
2. Clone your new repository

`Dockerfile`: Contains build instructions... this is where you put pip packages you want installed
`Dockerfile.build`: a script for building and tagging the image
`Dockerfile.run`: an easy way to run 

Adjust `pip install` line in `Dockerfile` and then run `./build`

## Running:

The code is held in `main.py` though this can be adjusted in `Dockerfile`.  To run the project:

`./run`

To get an interactive python shell

`./run python`

Any args you pass to `Dockerfile.run` will be forwarded to the `docker run` command.

