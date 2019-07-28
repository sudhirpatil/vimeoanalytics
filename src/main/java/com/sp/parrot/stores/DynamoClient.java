package com.sp.parrot.stores;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import software.amazon.awssdk.services.dynamodb.model.*;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;

import java.util.HashMap;

import java.util.Map;

import software.amazon.awssdk.services.dynamodb.model.DynamoDbException;

public class DynamoClient {
    private static final Logger log = LogManager.getLogger(DynamoClient.class);

    Vertx vertx;
    DynamoDbClient ddb;
    JsonObject config;

    public DynamoClient(Vertx vertx, JsonObject config){
        this.vertx = vertx;
        this.config = config;
        ddb = DynamoDbClient.builder().build();

    }

    public Future<Void> saveMovieAsync(String tableName, JsonObject movieJson){
        Future<Void> future = Future.future();

        vertx.executeBlocking(
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
            log.info("Saved to DynamoDb, title: {}", itemValues.get("title").s());
            return response.sdkHttpResponse().statusCode();
        } catch (ResourceNotFoundException e) {
            log.error("Error: The table \"{}\" can't be found.\n", tableName);
            log.error("Be sure that it exists and that you've typed its name correctly!");
        } catch (DynamoDbException e) {
            log.error(e.getMessage());
        }
        return 500;
    }



}
