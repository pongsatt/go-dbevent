test:
	go test
testreport:
	go test -coverprofile=coverage.out
	go tool cover -html=coverage.out 
genmock:
	go get github.com/vektra/mockery/v2/.../
	mockery --name=Backoffer --inpackage
	mockery --name=ConsumerDriver --inpackage