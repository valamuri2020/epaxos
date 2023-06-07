all: build run

build:
	go install master
	go install server
	go install client

clean:
	rm -rf bin/