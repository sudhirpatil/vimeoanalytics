package com.sp.parrot;

import io.vertx.config.ConfigRetriever;
import io.vertx.config.ConfigRetrieverOptions;
import io.vertx.config.ConfigStoreOptions;
import io.vertx.core.*;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.asyncsql.MySQLClient;
import io.vertx.ext.jdbc.JDBCClient;
import io.vertx.ext.sql.SQLClient;
import io.vertx.ext.sql.SQLConnection;
import io.vertx.ext.web.client.HttpRequest;
import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.client.WebClientOptions;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class Videos  extends AbstractVerticle {
    private static final Logger log = LogManager.getLogger(Videos.class);

    WebClient client;
    protected static  String accessToken;

    // Convenience method so you can run it in your IDE
    public static void main(String[] args) {
        // TODO:: implement fetching https://github.com/vert-x3/vertx-examples/blob/master/web-client-examples/src/main/java/io/vertx/example/webclient/oauth/TwitterOAuthExample.java
        accessToken = "87f6e24bf44bb198227ca7a176d90d6b";
        Runner.runExample(Videos.class);
    }

    @Override
    public void start() throws Exception {
        // Get all movie & search term from mysql
            // For each Movie, search all Videos
            // Combine comments, likes , views for each movie
            // Save each movie result to dynamo with movie-id & current hour as key.
            // Save search results to s3
        // Create the web client and enable SSL/TLS with a trust store
//        JsonObject mySQLClientConfig = new JsonObject()
//                .put("username", "parrotplay")
//                .put("password", "playp3n")
//                .put("database", "parrotplay")
//                .put("port", 3306)
//                .put("host", "parrot-dev-combined-cluster-1.cluster-cnhweqsnlq6l.us-east-1.rds.amazonaws.com");
//
//        log.info("creating mysql connection:  {}", mySQLClientConfig);
//        SQLClient mySQLClient = MySQLClient.createShared(vertx, mySQLClientConfig);
//        mySQLClient.getConnection(res -> {
//            if (res.succeeded()) {
//
//                SQLConnection connection = res.result();
//                log.info("mysql connection created");
//            } else {
//                res.cause().printStackTrace();
//            }
//        });

        ConfigRetriever retriever = ConfigRetriever
                .create(vertx, new ConfigRetrieverOptions()
                        .addStore(new ConfigStoreOptions().setType("file").
                                setConfig(new JsonObject().put("path", "./../../../../../../conf/config.json"))));

        retriever.getConfig(ar -> {
            if (ar.failed()) {
                ar.cause().printStackTrace();
                // Failed to retrieve the configuration
            } else {
                JsonObject config = ar.result();
                log.info(config);
                log.info("creating mysql connection");
                SQLClient mySQLClient = MySQLClient.createShared(vertx, config);
                mySQLClient.getConnection(res -> {
                    if (res.succeeded()) {
                        log.info("mysql connection created");
                        SQLConnection connection = res.result();
                        connection
                                .query("SELECT * FROM search_term_movies limit 2", result -> {
                                    log.info(result.result().getResults());
                                    log.info(result.result().getColumnNames());
                                        });
//                        SELECT * FROM parrotplay.serch_term_movies
                    } else {
                        res.cause().printStackTrace();
                    }
                });
            }
        });
//        SQLClient client = JDBCClient.createShared(vertx, config);
//        client = WebClient.create(vertx,
//                new WebClientOptions()
//                        .setSsl(true)
//        );
//
//        ThrottledRequests executor = new ThrottledRequests(vertx);
//        String movieId = "1234", queryTerm = "spider";
//        searchVideos(executor, "spider", 5)
//            .compose(this::getVideoMetadata)
//            .setHandler(async -> {
//                if(async.succeeded()){
//                    log.info("VideoResponse : {}", async.result());
//                }else {
//                    async.cause().printStackTrace();
//                }
//                vertx.close();
//            });
    }

    private Future<JsonObject> getVideoMetadata(List<Buffer> listBuffer) {
        Future<JsonObject> future = Future.future();

        log.info("extracting video metadata");
        int totalComments = 0, totalLikes =0, totalViews =0;
        for(Buffer buffer: listBuffer){
            JsonArray jsonArray = buffer.toJsonObject().getJsonArray("data");
            for(Object json : jsonArray){
                int comments = 0, likes =0, views =0;
                JsonObject videoData = (JsonObject)json;

                if(videoData.containsKey("metadata") && videoData.getJsonObject("metadata").containsKey("connections")) {
                    JsonObject connections = videoData.getJsonObject("metadata").getJsonObject("connections");
                    if(connections.containsKey("comments") && connections.getJsonObject("comments").containsKey("total")) {
                        comments = connections.getJsonObject("comments").getInteger("total");
                        totalComments += comments;
                    }

                    if(connections.containsKey("likes")  && connections.getJsonObject("likes").containsKey("total")){
                        likes = connections.getJsonObject("likes").getInteger("total");
                        totalLikes += likes;
                    }

                    if(videoData.containsKey("stats") && connections.getJsonObject("stats") != null && connections.getJsonObject("stats").containsKey("plays")){
                        String viewsStr = connections.getJsonObject("stats").getString("plays");
                        log.info("viewsStr : {}", viewsStr);
                        if(viewsStr != null) {
                            views = Integer.valueOf(viewsStr);
                            totalViews += views;
                        }
                    }

                    String uri = videoData.getString("uri");
                    log.info("uri : {}, totalComments : {} , totalLikes : {}, views : {}", uri, comments, likes, views);
                }
            }
        }

        JsonObject videoStat = new JsonObject()
                .put("totalComments", totalComments)
                .put("totalLikes", totalLikes)
                .put("totalViews", totalViews);
//                .put("movieId", movieId)
//                .put("queryTerm", queryTerm);
//                log.info("video stat json : {}", videoStat);
        future.complete(videoStat);
        return future;
    }

    public Future<List<Buffer>> searchVideos(ThrottledRequests executor, String queryTerm, int totalPages ){
        Future<List<Buffer>> result = Future.future();

        List<Future> futures =
                IntStream
                .range(1, totalPages + 1)
                .mapToObj(pageNum -> { // Create request object for each page
                    log.info("Searching queryTerm:"+queryTerm+"  page:"+pageNum);
                    return client.get(443, "api.vimeo.com", "/videos")
//                .putHeader("Authorization", "basic "+accessToken)
                            .addQueryParam("access_token", accessToken)
                            .addQueryParam("query", queryTerm)
                            .addQueryParam("page", String.valueOf(pageNum))
                            .addQueryParam("per_page", "20")
                            .addQueryParam("direction", "asc");
                })
                .map(request -> { // execute each request i.e add to queue for processing
                    Future<Buffer> future = Future.future();
                    executor
                            .execute(request)
                            .setHandler(async -> { // response handler sets response to future object
                                if(async.succeeded()){
                                    log.info("Got response from execute() :{}");
                                    future.complete(async.result());
                                }else {
                                    future.fail(async.cause());
                                }
                            });

                    return future;
                })
                .collect(Collectors.toList());

        CompositeFuture
                .all(futures) // Combine all future results success if all futures complete successfully, fail even if one future fails
                .setHandler(joinFutures(result));

        return result;
    }

    // composite response handler to add all responses to request to Future<List>
    private Handler<AsyncResult<CompositeFuture>> joinFutures(Future<List<Buffer>> future) {
        log.info("CompositeFuture async handler");
        return async -> {
            if (async.succeeded()) {
                CompositeFuture compositeFuture = async.result();
                Collection resultBuffers = IntStream
                        .range(0, compositeFuture.size())
                        .mapToObj(compositeFuture::resultAt)
                        .map(result -> {
                            log.info(" result before collecting : {}");
                            return result;
                        })
                        .collect(Collectors.toList());

                List<Buffer> total = new ArrayList<>();
                total.addAll(resultBuffers);

                future.complete(total);
            } else {
                future.fail(async.cause());
            }
        };
    }

}
