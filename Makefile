all:
	cd engine; make; cd -
	cd SubgraphMatch; make; cd -

clean:
	cd engine; make clean; cd -
	cd SubgraphMatch; make clean; cd -
	bin/clean-output
