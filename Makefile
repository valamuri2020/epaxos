build:
	go install server
	go install client

clean:
	rm *.out client*.log