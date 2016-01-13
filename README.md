# DynamoDbBatchWriteItemManagedRetry
The DynamoDB BatchWriteItem API puts or deletes multiple items in one or more tables in a single API call. If any requested operations fail because the table's provisioned throughput is exceeded or an internal processing failure occurs, the failed operations are returned in the UnprocessedItem response parameter.

DynamoDbBatchWriteItemManagedRetry will re-process the failed operations that are returned in the UnprocessItem response parameter.

## Usage
```java
final DocumentTable documentTable = new DocumentTable(accessKey, secretKey);
documentTable.batchSaveItem(itemList);
```
or
```java
final DocumentTable documentTable = new DocumentTable("/usr/project/awsCredentials.properties");
documentTable.batchSaveItem(itemList);
```
or
```java
final DocumentTable documentTable = new DocumentTable(); // will load awsCredentials.properties from default location /usr/portalbackendengine/awsCredentials.properties
documentTable.batchSaveItem(itemList);
```
or
```java
final DocumentTable documentTable = new DocumentTable(client); // your own AmazonDynamoDBClient client
documentTable.batchSaveItem(itemList);
```
or
```java
final DocumentTable documentTable = new DocumentTable(awsCredentials); // your own AWSCredentials credential
documentTable.batchSaveItem(itemList);
```
## Limitations
In development phase

