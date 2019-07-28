package com.sp.parrot;

import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DynamoClientExample {
    private static final Logger log = LogManager.getLogger(DynamoClientExample.class);

    Vertx vertx;

    public static void listTables(){
        log.info("Your DynamoDB tables:\n");

        // snippet-start:[dynamodb.java2.list_tables.main]
        DynamoDbClient ddb = DynamoDbClient.create();

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

    public  static void saveMovie(String tableName, JsonObject movieJson){
        Map<String, Object> movieMap = movieJson.getMap();

        Map<String,AttributeValue> itemValues = new HashMap<String,AttributeValue>();
        for(Map.Entry<String, Object> movie: movieMap.entrySet()){
            itemValues.put(movie.getKey(), AttributeValue.builder().s(movie.getValue().toString()).build());
        }

        DynamoDbClient ddb = DynamoDbClient.create();
        PutItemRequest request = PutItemRequest.builder()
                .tableName(tableName)
                .item(itemValues)
                .build();

        try {
            ddb.putItem(request);
        } catch (ResourceNotFoundException e) {
            log.error("Error: The table \"{}\" can't be found.\n", tableName);
            log.error("Be sure that it exists and that you've typed its name correctly!");
            System.exit(1);
        } catch (DynamoDbException e) {
            log.error(e.getMessage());
            System.exit(1);
        }
        // snippet-end:[dynamodb.java2.put_item.main]
        log.info("Save Done!");
        
    }
    
    public  static void putItem(String[] args){
        final String USAGE = "\n" +
                "Usage:\n" +
                "    PutItem <table> <name> [field=value ...]\n\n" +
                "Where:\n" +
                "    table    - the table to put the item in.\n" +
                "    name     - a name to add to the table. If the name already\n" +
                "               exists, its entry will be updated.\n" +
                "Additional fields can be added by appending them to the end of the\n" +
                "input.\n\n" +
                "Example:\n" +
                "    PutItem Cellists Pau Language=ca Born=1876\n";

        if (args.length < 2) {
            log.info(USAGE);
            System.exit(1);
        }

        String tableName = args[0];
        String title = args[1];
        ArrayList<String[]> extra_fields = new ArrayList<String[]>();

        // any additional args (fields to add to database)?
        for (int x = 2; x < args.length; x++) {
            String[] fields = args[x].split("=", 2);
            if (fields.length == 2) {
                extra_fields.add(fields);
            } else {
                log.info("Invalid argument: {}\n", args[x]);
                log.info(USAGE);
                System.exit(1);
            }
        }

        if (extra_fields.size() > 0) {
            log.info("Additional fields:");
            for (String[] field : extra_fields) {
                log.info("  {}: {}\n", field[0], field[1]);
            }
        }

        // snippet-start:[dynamodb.java2.put_item.main]
        HashMap<String,AttributeValue> item_values =
                new HashMap<String,AttributeValue>();

        log.info("Adding \"{}\" to \"{}\"", title, tableName);
        item_values.put("title", AttributeValue.builder().s(title).build());

        for (String[] field : extra_fields) {
            item_values.put(field[0], AttributeValue.builder().s(field[1]).build());
        }

        DynamoDbClient ddb = DynamoDbClient.create();
        PutItemRequest request = PutItemRequest.builder()
                .tableName(tableName)
                .item(item_values)
                .build();

        try {
            ddb.putItem(request);
        } catch (ResourceNotFoundException e) {
            log.error("Error: The table \"{}\" can't be found.\n", tableName);
            log.error("Be sure that it exists and that you've typed its name correctly!");
            System.exit(1);
        } catch (DynamoDbException e) {
            log.error(e.getMessage());
            System.exit(1);
        }
        // snippet-end:[dynamodb.java2.put_item.main]
        log.info("Done!");
    }

    public static void query(String[] args)
    {
        final String USAGE = "\n" +
                "Usage:\n" +
                "    Query <table> <partitionkey> <partitionkeyvalue>\n\n" +
                "Where:\n" +
                "    table - the table to put the item in.\n" +
                "    partitionkey  - partition key name of the table.\n" +
                "    partitionkeyvalue - value of the partition key that should match.\n\n" +
                "Example:\n" +
                "    Query GreetingsTable Language eng \n";

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


        // snippet-start:[dynamodb.java2.query.main]
        DynamoDbClient ddb = DynamoDbClient.builder().build();

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
        String[] items = {"spe-sudhir", "testMovie", "timestamp=2019-07-27", "comments=10", "likes=20"};
//        putItem(items);

        String[] queryArgs = {"spe-sudhir", "title", "title1"};
        query(queryArgs);

//        JsonObject movieJson = new JsonObject()
//                .put("title", "title1")
//                .put("timestamp", "2019-07-25 10:00:00")
//                .put("comments", 10)
//                .put("likes", 5);
//        saveMovie("spe-sudhir", movieJson);
    }
}
