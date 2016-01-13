# DynamoDbBatchWriteItemManagedRetry
Managed retry for DynamoDb BatchWriteItem API.

The DynamoDB BatchWriteItem API puts or deletes multiple items in one or more tables in a single API call. If any requested operations fail because the table's provisioned throughput is exceeded or an internal processing failure occurs, the failed operations are returned in the UnprocessedItem response parameter.

DynamoDbBatchWriteItemManagedRetry will re-process the failed operations that are returned in the UnprocessItem response parameter.

## Usage

```java
final DocumentTable documentTable = new DocumentTable(accessKey, secretKey);
documentTable.batchSaveItem(itemList);
```

## Limitations

In development phase

