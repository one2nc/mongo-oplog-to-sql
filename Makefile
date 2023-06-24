.PHONY: setup
setup:
	docker-compose up -d

.PHONY: setup-down
setup-down:
	docker-compose down --volumes

.PHONY: connect
connect:
	docker exec -it mongo-oplog-sql-db psql -U postgres -d postgres

.PHONY: build 
build:
	go build -o MongoOplogToSQL ./cmd/oplog-parser
