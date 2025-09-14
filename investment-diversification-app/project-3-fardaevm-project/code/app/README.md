## Getting Started

1. Setup Virtual Environment
`python3 -m venv .venv`

2. Start Virtual Environment
`.venv/bin/activate` (command varies per machine, this works for MacOS)

3. Install Dependencies
`pip install -r requirements.txt`

## Run
Start: `flask run`




## Using Makefile

Set the neo4j password and IP address in your shell. These are required to make remote calls to neo4j.

```bash
export N4J_IP=127.0.0.1
export N4J_PW=example

```

Run `make run`


See the `Makefile` for details.