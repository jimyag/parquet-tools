build:
	go build -o bin/parquet-tools -trimpath -v ./
release:
	go build -o parquet-tools -trimpath -v  ./