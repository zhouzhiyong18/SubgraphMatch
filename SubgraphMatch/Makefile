# ----------------------------------------------------------------------
# compiler options
# ----------------------------------------------------------------------

CXX= g++

CFLAGS_COMMON=-std=c++0x -g -O2 -I${HADOOP_HOME}/include -I${GRAPHLITE_HOME}/include
LIB_GRAPHALGO=-fPIC -shared

# ----------------------------------------------------------------------
# target
# ----------------------------------------------------------------------

all : example

# ----------------------------------------------------------------------
# example graph algorithms
# ----------------------------------------------------------------------

EXAMPLE_ALGOS=SubgraphMatch.so

example: ${EXAMPLE_ALGOS}

%.so : %.cc
	${CXX} ${CFLAGS_COMMON} $< ${LIB_GRAPHALGO} -o $@

SubgraphMatch.so : SubgraphMatch.cc

# ----------------------------------------------------------------------
# clean up
# ----------------------------------------------------------------------

clean :
	rm -rf *.so
