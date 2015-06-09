# Bootstrap Dockerizing

**Not finished, yet**.

The purpose of this script is to bootstrap and later control various dockerizing container.

## Usage Instruction
#### Bootstrapping

A Dockerizing semantic web application can consist of several parts.  

`./bootsrap.sh virtuoso` downloads just the Virtuoso repository and builds the docker.  
With `./bootsrap.sh virtuoso backup ontowiki` all 3 repositories will be dowloaded and will be built. This is the same as `./bootsrap.sh v o b`.  

The first parameter (e.g. `virtuoso`) tells the script which main part should be built. All other available parameters (e.g. `ontowiki` and/or  `backup`) are build in dependence of the first main parameter (e.g. `virtuoso`). 

#### Running

**Coming soon**.
