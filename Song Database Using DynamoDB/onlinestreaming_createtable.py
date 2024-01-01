#!/usr/bin/env python3
import boto3

def create_table(
        ddb_table_name,
        partition_key,
        sort_key,
        LSI_SK1,
        GSI_PK1,
        GSI_SK1,
        GSI_PK2,
        GSI_SK2,
        GSI_PK3,
        GSI_SK3,
        GSI_PK4,
        GSI_SK4,
        GSI_PK5,
        GSI_SK5
        ):

#LSI_SK1 refers to the range key for our local secondary index.
#GSI_PK1 and GSI_SK1 refer to the hash and range key for our global secondary index.
#GSI_PK2 and GSI_SK2 refer to the hash and range key for our global secondary index.
#GSI_PK3 and GSI_SK3 refer to the hash and range key for our global secondary index.
#GSI_PK4 and GSI_SK4 refer to the hash and range key for our global secondary index.
#GSI_PK5 and GSI_SK5 refer to the hash and range key for our global secondary index.

    dynamodb = boto3.resource('dynamodb')

    table_name = ddb_table_name
    
    attribute_definitions = [
        {'AttributeName': partition_key, 'AttributeType': 'S'},
        {'AttributeName': sort_key, 'AttributeType': 'S'},
        {'AttributeName': LSI_SK1, 'AttributeType': 'S'},
        {'AttributeName': GSI_PK1, 'AttributeType': 'S'},
        {'AttributeName': GSI_SK1, 'AttributeType': 'S'},
        {'AttributeName': GSI_PK2, 'AttributeType': 'S'},
        {'AttributeName': GSI_SK2, 'AttributeType': 'S'},
        {'AttributeName': GSI_PK3, 'AttributeType': 'S'},
        {'AttributeName': GSI_SK3, 'AttributeType': 'S'},
        {'AttributeName': GSI_PK4, 'AttributeType': 'S'},
        {'AttributeName': GSI_SK4, 'AttributeType': 'S'},
        {'AttributeName': GSI_PK5, 'AttributeType': 'S'},
        {'AttributeName': GSI_SK5, 'AttributeType': 'S'}
        ]
    
    key_schema = [{'AttributeName': partition_key, 'KeyType': 'HASH'}, 
                  {'AttributeName': sort_key, 'KeyType': 'RANGE'}]
                  
    provisioned_throughput = {'ReadCapacityUnits': 5, 'WriteCapacityUnits': 10}
    
    lsi = [{
        'IndexName': 'Stream_Index',
        'KeySchema': [
            {'AttributeName': partition_key, 'KeyType': 'HASH'},
            {'AttributeName': LSI_SK1, 'KeyType': 'RANGE'}],
        'Projection': {'ProjectionType': 'ALL'}
    }]

    gsi = [{
            'IndexName': 'Stream_Activity',
            'KeySchema': [
                {'AttributeName': GSI_PK1, 'KeyType': 'HASH'},
                {'AttributeName': GSI_SK1, 'KeyType': 'RANGE'}],
            'Projection': {'ProjectionType': 'INCLUDE',
                           'NonKeyAttributes': ['username']
            },
            'ProvisionedThroughput': {'ReadCapacityUnits': 5, 'WriteCapacityUnits': 10}
    },
    {
            'IndexName': 'Artist_Index',
            'KeySchema': [
                {'AttributeName': GSI_PK2, 'KeyType': 'HASH'},
                {'AttributeName': GSI_SK2, 'KeyType': 'RANGE'}],
            'Projection': {'ProjectionType': 'INCLUDE',
                           'NonKeyAttributes': ['song_id', 'album_name', 'duration']
            },
            'ProvisionedThroughput': {'ReadCapacityUnits': 5, 'WriteCapacityUnits': 10}
    },
    {
            'IndexName': 'Album_Index',
            'KeySchema': [
                {'AttributeName': GSI_PK3, 'KeyType': 'HASH'},
                {'AttributeName': GSI_SK3, 'KeyType': 'RANGE'}],
            'Projection': {'ProjectionType': 'INCLUDE',
                           'NonKeyAttributes': ['song_id', 'duration']
            },
            'ProvisionedThroughput': {'ReadCapacityUnits': 5, 'WriteCapacityUnits': 10}
    },
    {
            'IndexName': 'Producer_Index',
            'KeySchema': [
                {'AttributeName': GSI_PK4, 'KeyType': 'HASH'},
                {'AttributeName': GSI_SK4, 'KeyType': 'RANGE'}],
            'Projection': {'ProjectionType': 'INCLUDE',
                           'NonKeyAttributes': ['artist_name', 'album_name']
            },
            'ProvisionedThroughput': {'ReadCapacityUnits': 5, 'WriteCapacityUnits': 10}
    },
    {
            'IndexName': 'Genre_Index',
            'KeySchema': [
                {'AttributeName': GSI_PK5, 'KeyType': 'HASH'},
                {'AttributeName': GSI_SK5, 'KeyType': 'RANGE'}],
            'Projection': {'ProjectionType': 'INCLUDE',
                           'NonKeyAttributes': ['album_name']
            },
            'ProvisionedThroughput': {'ReadCapacityUnits': 5, 'WriteCapacityUnits': 10}
    }]
    
    try:
        # Create a DynamoDB table with the parameters provided
        table = dynamodb.create_table(TableName=table_name,
                                      KeySchema=key_schema,
                                      AttributeDefinitions=attribute_definitions,
                                      ProvisionedThroughput=provisioned_throughput,
                                      LocalSecondaryIndexes=lsi,
                                      GlobalSecondaryIndexes=gsi
                                      )
        return table
    except Exception as err:
        print("{0} Table could not be created".format(table_name))
        print("Error message {0}".format(err))

#The following function allows us to delete the table if needed.

def delete_table(name):
    dynamodb = boto3.resource('dynamodb')  
    table = dynamodb.Table(name)
    table.delete()

#The create_table function here is now called. We have six secondary indexes: one LSI and five GSIs.

if __name__ == '__main__':
    table = create_table("onlinestreaming", "PK", "SK", "LSI_SK1", "GSI_PK1", "GSI_SK1", "GSI_PK2", "GSI_SK2", "GSI_PK3", "GSI_SK3", "GSI_PK4", "GSI_SK4", "GSI_PK5", "GSI_SK5")