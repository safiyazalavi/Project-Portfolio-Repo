_default:  # Default print each make command and some docstring
	@grep -n -v 'grep'  Makefile | grep ':  #*' -

run:  # run the flask app with envionment variables
	PYTHONPATH=../scripts FLASK_DEBUG=1 N4J_IP=${N4J_IP} N4J_PW=${N4J_PW} flask run

build:
	PYTHONPATH=../scripts python3 -m graph.build_graph

