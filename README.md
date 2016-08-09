# ctrl_iip_sim

This code is the NCSA LSST data management interns' interpretation of the image ingest and processing architecture currently being worked through. This code is not, in any way, an architectural proposal for the actual system, just a simplified implementation for the interns to work with to test ideas and better understand the problem.

## Installation

- Create a new Fedora 22 Cloud instance on OpenStack
- Download the project folder to the home directory
- Run `sudo ./install.sh`
- Copy the private key for the machine to `/lsst/` and name it `rsync_key` (permissions: 0400)
- Start the simulator with `./run.sh`

## Usage

- Run the simulator with `./run.sh`
- Use `./refresh.sh` if the machine's IP address changes or something goes wrong
- If there is a major problem, use `./stop.sh` to close everything

## Credit
Actual ctrl_iip found at https://github.com/lsst/ctrl_iip
