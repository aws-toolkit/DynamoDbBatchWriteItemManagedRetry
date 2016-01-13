package com.github.awstoolkit.dynamodbbatchwriteitemmanagedretry;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.PropertiesFileCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.document.BatchWriteItemOutcome;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.KeyAttribute;
import com.amazonaws.services.dynamodbv2.document.PrimaryKey;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.TableWriteItems;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.DescribeTableResult;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import com.amazonaws.services.dynamodbv2.model.ScanResult;
import com.amazonaws.services.dynamodbv2.model.WriteRequest;
import com.google.common.collect.Lists;

/**
 * 
 * @author Oh Chin Boon
 * @since 1.0.0
 */
public class DocumentTable {
	private static Logger LOG = LoggerFactory.getLogger(DocumentTable.class);

	protected DynamoDB dynamoDb;
	private AmazonDynamoDBClient client;
	private Table table;

	private String tableName;

	private Region operatingRegion;

	public static final int DYNAMODB_LIMITS_BATCH_WRITE_MAX = 25;

	/**
	 * The default AWS Crendentials Properties File.
	 * 
	 * @author Oh Chin Boon
	 * @since 1.0.0
	 */
	public static final String DEFAULT_AWS_CREDENTIALS_PROPERTIES_FILE = "/usr/portalbackendengine/awsCredentials.properties";

	/**
	 * Calling this constructor will initialize DocumentTable using an Aws
	 * Credentials Properties file located at
	 * {@link DocumentTable#DEFAULT_AWS_CREDENTIALS_PROPERTIES_FILE}.
	 * 
	 * @author Oh Chin Boon
	 * @since 1.0.0
	 */
	public DocumentTable() {
		this.initializeDynamoDb(DEFAULT_AWS_CREDENTIALS_PROPERTIES_FILE);
	}

	/**
	 * Calling this constructor will initialize DocumentTable using
	 * 
	 * @param secretKey
	 * @param accessKey
	 */
	public DocumentTable(final String accessKey, final String secretKey) {
		this.initializeDynamoDb(accessKey, secretKey);
	}

	/**
	 * Calling this constructor will initialize the DynamoDb client and Table.
	 * 
	 * @author Oh Chin Boon
	 * @param awsCredentialPropertiesFilePath
	 * @since 1.0.0
	 */
	public DocumentTable(final String awsCredentialPropertiesFilePath) {
		this.initializeDynamoDb(awsCredentialPropertiesFilePath);
	}

	/**
	 * Calling this constructor will initialize with an
	 * {@link AmazonDynamoDBClient}.
	 * 
	 * @author Oh Chin Boon
	 * @param client
	 * @since 1.0.0
	 */
	public DocumentTable(final AmazonDynamoDBClient client) {
		this.client = client;
	}

	public DocumentTable(final AWSCredentials awsCredentials) {
		this.initializeDynamoDb(awsCredentials);
	}

	/**
	 * Sets the operating region.
	 * 
	 * @author Oh Chin Boon
	 * @param region
	 * @since 1.0.0
	 */
	public void setRegion(final Region region) {
		this.operatingRegion = region;

		this.client.setRegion(this.operatingRegion);
	}

	/**
	 * Fluent method to set the operating region.
	 * 
	 * @author Oh Chin Boon
	 * @param region
	 * @return
	 * @since 1.0.0
	 */
	public DocumentTable withRegion(final Region region) {
		this.setRegion(region);

		return this;
	}

	/**
	 * Initializes the {@link DocumentTable} with a specified AWS Credentials
	 * properties file path.
	 * 
	 * @author Oh Chin Boon
	 * @param awsCredentialPropertiesFilePath
	 * @since 1.0.0
	 */
	private void initializeDynamoDb(final String awsCredentialPropertiesFilePath) {
		final AWSCredentials credentials = new PropertiesFileCredentialsProvider(awsCredentialPropertiesFilePath)
				.getCredentials();

		this.initializeDynamoDb(credentials);
	}

	private void initializeDynamoDb(final AWSCredentials awsCredentials) {
		final AmazonDynamoDBClient client = new AmazonDynamoDBClient(awsCredentials);

		this.client = client;
	}

	/**
	 * Initializes a {@link DynamoDB} object.
	 * 
	 * @author Oh Chin Boon
	 * @param accessKey
	 * @param secretKey
	 * @since 1.0.0
	 */
	private void initializeDynamoDb(final String accessKey, final String secretKey) {
		final AWSCredentials credentials = new BasicAWSCredentials(accessKey, secretKey);

		this.initializeDynamoDb(credentials);
	}

	public List<BatchWriteItemOutcome> batchSaveItem(final Item... items) {
		// convert array into list
		return this.batchSaveItem(Arrays.asList(items));
	}

	public List<BatchWriteItemOutcome> batchSaveItem(final List<Item> itemList) {
		// partition
		final List<List<Item>> listOfItemList = Lists.partition(itemList, DYNAMODB_LIMITS_BATCH_WRITE_MAX);

		final List<BatchWriteItemOutcome> batchWriteItemOutcomeList = new ArrayList<BatchWriteItemOutcome>();

		int unprocessedItemSize = 0;

		for (final List<Item> partitionedItemList : listOfItemList) {
			LOG.debug("Batch write starts...");
			final BatchWriteItemOutcome batchWriteItemOutcome = this.dynamoDb
					.batchWriteItem(new TableWriteItems(tableName).withItemsToPut(partitionedItemList));
			LOG.debug("Batch write ends.");

			batchWriteItemOutcomeList.add(batchWriteItemOutcome);

			for (final Map.Entry<String, List<WriteRequest>> entry : batchWriteItemOutcome.getUnprocessedItems()
					.entrySet()) {

				for (final WriteRequest request : entry.getValue()) {
					unprocessedItemSize++;
				}
			}
		}

		LOG.warn("There are [{}] unprocessed items at the end of the DynamoDb batch save operation",
				unprocessedItemSize);

		if (unprocessedItemSize > 0) {
			// retry batch save
			this.processUnprocessedBatchSave(batchWriteItemOutcomeList);
		}

		return batchWriteItemOutcomeList;
	}

	private void processUnprocessedBatchDelete(final List<BatchWriteItemOutcome> batchWriteItemOutcomeList) {
		// need to know the hash and range key
		final DescribeTableResult describeTableResult = this.describeTable();

		final String hashKeyName = this.getHashKeyNameHelper(describeTableResult);
		final String rangeKeyName = this.getRangeKeyNameHelper(describeTableResult);

		final List<Map<String, AttributeValue>> unprocessedRecordList = new ArrayList<Map<String, AttributeValue>>();

		for (final BatchWriteItemOutcome batchWriteItemOutcome : batchWriteItemOutcomeList) {
			if (!batchWriteItemOutcome.getUnprocessedItems().isEmpty()) {
				// there are unprocessed items
				final Map<String, List<WriteRequest>> unprocessItemsMap = batchWriteItemOutcome.getUnprocessedItems();

				for (final Map.Entry<String, List<WriteRequest>> entry : unprocessItemsMap.entrySet()) {

					for (final WriteRequest request : entry.getValue()) {
						unprocessedRecordList.add(request.getDeleteRequest().getKey());
					}
				}
			}
		}

		final List<PrimaryKey> unprocessedPrimaryKeyList = this
				.transformAttributeValueMapListToPrimaryKeyList(unprocessedRecordList, hashKeyName, rangeKeyName);

		LOG.debug("Going to process unprocessed delete records now");
		this.batchDeleteItem(unprocessedPrimaryKeyList);
	}

	private void processUnprocessedBatchSave(final List<BatchWriteItemOutcome> batchWriteItemOutcomeList) {
		// need to know the hash and range key
		final DescribeTableResult describeTableResult = this.describeTable();

		final String hashKeyName = this.getHashKeyNameHelper(describeTableResult);
		final String rangeKeyName = this.getRangeKeyNameHelper(describeTableResult);

		final List<Map<String, AttributeValue>> unprocessedRecordList = new ArrayList<Map<String, AttributeValue>>();

		for (final BatchWriteItemOutcome batchWriteItemOutcome : batchWriteItemOutcomeList) {
			if (!batchWriteItemOutcome.getUnprocessedItems().isEmpty()) {
				// there are unprocessed items
				final Map<String, List<WriteRequest>> unprocessItemsMap = batchWriteItemOutcome.getUnprocessedItems();

				for (final Map.Entry<String, List<WriteRequest>> entry : unprocessItemsMap.entrySet()) {

					for (final WriteRequest request : entry.getValue()) {
						unprocessedRecordList.add(request.getPutRequest().getItem());
					}
				}
			}
		}

		// transform List<Map<String, AttributeValue>> into List<Item>
		final List<Item> unprocessItemList = transformAttributeValueMapListIntoItemList(unprocessedRecordList,
				hashKeyName, rangeKeyName);

		LOG.debug("Going to process unprocessed save records now");
		this.batchSaveItem(unprocessItemList);
	}

	private List<Item> transformAttributeValueMapListIntoItemList(
			final List<Map<String, AttributeValue>> attributeValueMapList, final String hashKeyName,
			final String rangeKeyName) {
		final List<Item> itemList = new ArrayList<Item>(attributeValueMapList.size());

		for (final Map<String, AttributeValue> attributeValueMap : attributeValueMapList) {
			itemList.add(this.transformAttributeValueMapIntoItem(attributeValueMap, hashKeyName, rangeKeyName));
		}

		return itemList;
	}

	/**
	 * Transforms Attribute Value Map into Item.
	 * 
	 * @param attributeValueMap
	 * @param hashKeyName
	 * @param rangeKeyName
	 * @return
	 * @since 1.0.0
	 */
	private Item transformAttributeValueMapIntoItem(final Map<String, AttributeValue> attributeValueMap,
			final String hashKeyName, final String rangeKeyName) {
		final Item item = new Item();

		// build primary key
		final PrimaryKey primaryKey = new PrimaryKey();
		// hash key is always present
		// TODO assume String now
		primaryKey.addComponent(hashKeyName, attributeValueMap.get(hashKeyName).getS());

		// add range key if present
		// TODO assume String now
		if (rangeKeyName != null && attributeValueMap.containsKey(rangeKeyName)) {
			primaryKey.addComponent(rangeKeyName, attributeValueMap.get(rangeKeyName).getS());
		}

		// add primary key to item
		item.withPrimaryKey(primaryKey);

		// add non PK items
		// loop through map
		final Set<Entry<String, AttributeValue>> attributeValueSet = attributeValueMap.entrySet();

		final Iterator<Map.Entry<String, AttributeValue>> iterator = attributeValueSet.iterator();

		while (iterator.hasNext()) {
			final Entry<String, AttributeValue> entry = iterator.next();

			// if hash and range key, do not add again to item
			if (entry.getKey().equals(hashKeyName) || entry.getKey().equals(rangeKeyName)) {
				// skip processing this attributeValue
				continue;
			}

			// TODO for now expect only string data types
			item.withString(entry.getKey(), entry.getValue().getS());
		}

		return item;
	}

	public List<BatchWriteItemOutcome> batchDeleteItem(final List<PrimaryKey> primaryKeyList) {
		// partition
		final List<List<PrimaryKey>> listOfPrimaryKeyList = Lists.partition(primaryKeyList,
				DYNAMODB_LIMITS_BATCH_WRITE_MAX);

		final List<BatchWriteItemOutcome> batchWriteItemOutcomeList = new ArrayList<BatchWriteItemOutcome>();

		int unprocessedItemSize = 0;

		for (final List<PrimaryKey> partitionedPrimaryKeyList : listOfPrimaryKeyList) {
			final BatchWriteItemOutcome batchWriteItemOutcome = this.dynamoDb
					.batchWriteItem(new TableWriteItems(tableName).withPrimaryKeysToDelete(
							partitionedPrimaryKeyList.toArray(new PrimaryKey[partitionedPrimaryKeyList.size()])));

			batchWriteItemOutcomeList.add(batchWriteItemOutcome);

			for (final Map.Entry<String, List<WriteRequest>> entry : batchWriteItemOutcome.getUnprocessedItems()
					.entrySet()) {

				for (final WriteRequest request : entry.getValue()) {
					unprocessedItemSize++;
				}
			}
		}

		LOG.warn("There are [{}] unprocessed items at the end of the DynamoDb batch delete operation",
				unprocessedItemSize);

		if (unprocessedItemSize > 0) {
			// retry batch save
			this.processUnprocessedBatchDelete(batchWriteItemOutcomeList);
		}

		return batchWriteItemOutcomeList;
	}

	public DescribeTableResult describeTable() {
		// describe table get hash key
		final DescribeTableResult describeTableResult = this.client.describeTable(this.tableName);

		return describeTableResult;
	}

	public String getHashKeyNameHelper(final DescribeTableResult describeTableResult) {
		final List<KeySchemaElement> keySchemeElementList = describeTableResult.getTable().getKeySchema();

		if (keySchemeElementList != null) {
			for (final KeySchemaElement keySchemeElement : keySchemeElementList) {
				if (KeyType.HASH.toString().equals(keySchemeElement.getKeyType())) {
					// this is a hash key
					return keySchemeElement.getAttributeName();
				}
			}
		}

		// hash key not found - not likely to happen
		return null;
	}

	public String getRangeKeyNameHelper(final DescribeTableResult describeTableResult) {
		final List<KeySchemaElement> keySchemeElementList = describeTableResult.getTable().getKeySchema();

		if (keySchemeElementList != null) {
			for (final KeySchemaElement keySchemeElement : keySchemeElementList) {
				if (KeyType.RANGE.toString().equals(keySchemeElement.getKeyType())) {
					// this is a hash key
					return keySchemeElement.getAttributeName();
				}
			}
		}

		// range key not found - not likely to happen
		return null;
	}

	public void truncateTable() {
		final DescribeTableResult describeTableResult = this.describeTable();

		final String hashKeyName = this.getHashKeyNameHelper(describeTableResult);
		final String rangeKeyName = this.getRangeKeyNameHelper(describeTableResult);

		// get all data

		// TODO get provisioned throughput to decide scan fetch limit

		// do a capped scan (limit) to reduce provisioned throughput
		Map<String, AttributeValue> lastKeyEvaluated = null;

		// each map is a record.
		final List<Map<String, AttributeValue>> itemMapList = new ArrayList<Map<String, AttributeValue>>();

		do {
			// limit of 20 records per fetch should
			ScanRequest scanRequest = new ScanRequest().withTableName(tableName).withLimit(10)
					.withExclusiveStartKey(lastKeyEvaluated);

			ScanResult result = client.scan(scanRequest);

			// add records to list
			itemMapList.addAll(result.getItems());

			lastKeyEvaluated = result.getLastEvaluatedKey();
		} while (lastKeyEvaluated != null);

		// transform List<Map<String, AttributeValue>> into List<PrimaryKey>
		final List<PrimaryKey> primaryKeyList = this.transformAttributeValueMapListToPrimaryKeyList(itemMapList,
				hashKeyName, rangeKeyName);

		// batch delete
		this.batchDeleteItem(primaryKeyList);

	}

	private List<PrimaryKey> transformAttributeValueMapListToPrimaryKeyList(
			final List<Map<String, AttributeValue>> attributeValueMapList, final String hashKeyName,
			final String rangeKeyName) {
		// transform List<Map<String, AttributeValue>> into List<PrimaryKey>
		final List<PrimaryKey> primaryKeyList = new ArrayList<PrimaryKey>(attributeValueMapList.size());

		for (final Map<String, AttributeValue> itemMap : attributeValueMapList) {
			final PrimaryKey primaryKey = new PrimaryKey();

			// hash key is always present
			primaryKey.addComponents(new KeyAttribute(hashKeyName, itemMap.get(hashKeyName).getS()));

			// if range key is present (must be)
			if (rangeKeyName != null) {
				primaryKey.addComponents(new KeyAttribute(rangeKeyName, itemMap.get(rangeKeyName).getS()));
			}

			primaryKeyList.add(primaryKey);
		}

		return primaryKeyList;
	}
}