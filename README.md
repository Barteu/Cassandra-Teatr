# Cassandra-Theatre
SRDS project

### Create schema
`
docker run --rm --network cassandra -v "$(pwd)/scripts/create_schema.cql:/scripts/create_schema.cql" -e CQLSH_HOST=cassandra -e CQLSH_PORT=9042 -e CQLVERSION=3.4.6 nuvo/docker-cqlsh
`
### Load test data
`
docker run --rm --network cassandra -v "$(pwd)/scripts/load_data.cql:/scripts/load_data.cql" -e CQLSH_HOST=cassandra -e CQLSH_PORT=9042 -e CQLVERSION=3.4.6 nuvo/docker-cqlsh
`
### Drop schema
`
docker run --rm --network cassandra -v "$(pwd)/scripts/drop_schema.cql:/scripts/drop_schema.cql" -e CQLSH_HOST=cassandra -e CQLSH_PORT=9042 -e CQLVERSION=3.4.6 nuvo/docker-cqlsh
`
### Interactive cqlsh
`
docker run --rm -it --network cassandra nuvo/docker-cqlsh cqlsh cassandra 9042 --cqlversion='3.4.6'
`
