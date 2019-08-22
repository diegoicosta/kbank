### Confluent Kafka Platform

version used: 5.3.0

https://www.confluent.io/product/confluent-platform/

### Needed Topics

First, you must create all of this topics. 

```
kbank.command
kbank.account
kbank.account.accepted
kbank.account.refused
kbank.account.read
kbank.transfer
```

### Build the Application
Java 8, Maven 3.x.x

`mvn clean package`

### Running the Application
`java -jar ./target/kbank-1.0-spring-boot.jar` 
```
Server online at http://localhost:8080/
Press RETURN to stop...
```

### Testing the Application

#### Reading a Balance

``` 
curl -X GET \
  http://localhost:8080/account/ACC-855 \
  -H 'Content-Type: application/json' \
```
* Response

``` 
{
    "command": {
        "account": "ACC-855"
    },
    "id": "932a1e58-0a21-4358-b71c-7a6d35bb8c0c",
    "response": {
        "timestamp": 1566498425596,
        "status": "READ_ACCEPTED",
        "balance": 0
    }
}
```
#### Credit/Debit an Account

``` 
curl -X POST \
  http://localhost:8080/command \
  -H 'Content-Type: application/json' \
  -d '{
  "id": "4834af21-5970-4ddf-a9b0-b907ec033e14",
  "type": "CREDIT",
  "command": {
    "account": "ACC-855",
    "value": 852
  }
}'
```

``` 
curl -X POST \
  http://localhost:8080/command \
  -H 'Content-Type: application/json' \
  -d '{
  "id": "2c5b06a1-5527-4a10-9709-a40e9bafaa9c",
  "type": "DEBIT",
  "command": {
    "account": "ACC-855",
    "value": 234
  }
}'
```
