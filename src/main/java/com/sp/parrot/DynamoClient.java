package com.sp.parrot;

import io.vertx.config.ConfigRetriever;
import io.vertx.config.ConfigRetrieverOptions;
import io.vertx.config.ConfigStoreOptions;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.WorkerExecutor;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.asyncsql.MySQLClient;
import io.vertx.ext.sql.ResultSet;
import io.vertx.ext.sql.SQLClient;
import io.vertx.ext.sql.SQLConnection;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import software.amazon.awssdk.services.dynamodb.model.*;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;

import java.util.ArrayList;
import java.util.HashMap;

import java.util.List;
import java.util.Map;

import software.amazon.awssdk.services.dynamodb.model.DynamoDbException;
import software.amazon.awssdk.regions.Region;

public class DynamoClient {
    private static final Logger log = LogManager.getLogger(DynamoClient.class);

    Vertx vertx;
    DynamoDbClient ddb;

    DynamoClient(Vertx vertx){
        this.vertx = vertx;
        ddb = DynamoDbClient.builder().build();
    }

    public Future<Void> saveMovieAsync(String tableName, JsonObject movieJson){
        Future<Void> future = Future.future();
        WorkerExecutor executor = vertx.createSharedWorkerExecutor("my-worker-pool");
        executor.executeBlocking(
            promise -> { // blocking API handler
                int status = saveMovie(tableName, movieJson);
                promise.complete(status);
            }
            , false // Run parallel, ignore sequence
            , res -> { // Response handler
                System.out.println("The result is: " + res.result());
                future.complete();
        });
        return future;
    }

    public int saveMovie(String tableName, JsonObject movieJson){
        Map<String, Object> movieMap = movieJson.getMap();

        Map<String,AttributeValue> itemValues = new HashMap<String,AttributeValue>();
        for(Map.Entry<String, Object> movie: movieMap.entrySet()){
            itemValues.put(movie.getKey(), AttributeValue.builder().s(movie.getValue().toString()).build());
        }

        PutItemRequest request = PutItemRequest.builder()
                .tableName(tableName)
                .item(itemValues)
                .build();

        try {
            PutItemResponse response = ddb.putItem(request);
            log.info("Saved to DynamoDb, title: {}", itemValues.get("title"));
            return response.sdkHttpResponse().statusCode();
        } catch (ResourceNotFoundException e) {
            log.error("Error: The table \"{}\" can't be found.\n", tableName);
            log.error("Be sure that it exists and that you've typed its name correctly!");
        } catch (DynamoDbException e) {
            log.error(e.getMessage());
        }
        // snippet-end:[dynamodb.java2.put_item.main]
        return 500;
    }

    public void listTables(){
        log.info("Your DynamoDB tables:\n");

        boolean more_tables = true;
        String last_name = null;

        while(more_tables) {
            try {
                ListTablesResponse response = null;
                if (last_name == null) {
                    ListTablesRequest request = ListTablesRequest.builder().build();
                    response = ddb.listTables(request);
                }
                else {
                    ListTablesRequest request = ListTablesRequest.builder()
                            .exclusiveStartTableName(last_name).build();
                    response = ddb.listTables(request);
                }

                List<String> table_names = response.tableNames();

                if (table_names.size() > 0) {
                    for (String cur_name : table_names) {
                        log.info("* {}\n", cur_name);
                    }
                } else {
                    log.info("No tables found!");
                    System.exit(0);
                }

                last_name = response.lastEvaluatedTableName();
                if (last_name == null) {
                    more_tables = false;
                }
            } catch (DynamoDbException e) {
                log.error(e.getMessage());
                System.exit(1);
            }
        }
        // snippet-end:[dynamodb.java2.list_tables.main]
        log.info("\nDone!");
    }


    
    public void query(String[] args)
    {
        final String USAGE = "\n" +
                "Usage:\n" +
                "    <table> <partitionkey> <partitionkeyvalue>\n\n" +
                "Example:\n" +
                "    GreetingsTable Language eng \n";

        if (args.length < 3) {
            log.info(USAGE);
            System.exit(1);
        }

        String table_name = args[0];
        String partition_key_name = args[1];
        String partition_key_val = args[2];
        String partition_alias = "#a";

        log.info("Querying {}", table_name);
        log.info("");

        //set up an alias for the partition key name in case it's a reserved word
        HashMap<String,String> attrNameAlias =
                new HashMap<String,String>();
        attrNameAlias.put(partition_alias, partition_key_name);

        //set up mapping of the partition name with the value
        HashMap<String, AttributeValue> attrValues =
                new HashMap<String,AttributeValue>();
        attrValues.put(":"+partition_key_name, AttributeValue.builder().s(partition_key_val).build());

        QueryRequest queryReq = QueryRequest.builder()
                .tableName(table_name)
                .keyConditionExpression(partition_alias + " = :" + partition_key_name)
                .expressionAttributeNames(attrNameAlias)
                .expressionAttributeValues(attrValues).limit(5)
                .build();

        try {
            QueryResponse response = ddb.query(queryReq);
            log.info(response.count());
            for(Map<String, AttributeValue> item :response.items()){
                for(Map.Entry<String, AttributeValue> entry: item.entrySet()){

                    log.info("key {} , value {}", entry.getKey(), entry.getValue().s());
                }
            }
            log.info(response.items());
        } catch (DynamoDbException e) {
            log.error(e.getMessage());
            System.exit(1);
        }
        // snippet-end:[dynamodb.java2.query.main]
        log.info("Done!");
    }


    public static void main(String[] args)
    {
        DynamoClient dynamoClient = new DynamoClient(null);

        String[] queryArgs = {"spe-sudhir", "title", "mortal engines"};
        dynamoClient.query(queryArgs);

//        JsonObject movieJson = new JsonObject()
//                .put("title", "title1")
//                .put("timestamp", "2019-07-25 10:00:00")
//                .put("comments", 10)
//                .put("likes", 5);
//        dynamoClient.saveMovie("spe-sudhir", movieJson);
    }
}
