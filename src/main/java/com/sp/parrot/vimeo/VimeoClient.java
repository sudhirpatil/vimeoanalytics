package com.sp.parrot.vimeo;

import com.sp.parrot.api.VimeoApiClient;
import com.sp.parrot.utils.Utils;
import io.vertx.core.*;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.client.WebClientOptions;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class VimeoClient {
    private static final Logger log = LogManager.getLogger(VimeoClient.class);
    Vertx vertx;
    WebClient client;
    JsonObject config;
//    protected static  String accessToken = "87f6e24bf44bb198227ca7a176d90d6b";

    public VimeoClient(Vertx vertx, JsonObject config){
        this.vertx = vertx;
        client = WebClient.create(vertx,
                new WebClientOptions()
                        .setSsl(true)
        );
        this.config = config;
    }

    public Future<List<Buffer>> searchVideos(VimeoApiClient executor, String movieTitle, int totalPages ){
        Future<List<Buffer>> result = Future.future();

        //TODO:: query first page and calculate number of available pages, log error only if first page query fails
        List<Future> futures =
                IntStream
                        .range(1, totalPages + 1)
                        .mapToObj(pageNum -> { // Create request object for each page
                            log.info("Adding vimeo request to queue, queryTerm:"+movieTitle+"  page:"+pageNum);
                            return client.get(443, "api.vimeo.com", "/videos")
//                .putHeader("Authorization", "basic "+accessToken)
                                    .addQueryParam("access_token", config.getString("accessToken"))
                                    .addQueryParam("query", movieTitle)
                                    .addQueryParam("page", String.valueOf(pageNum))
                                    .addQueryParam("per_page", config.getString("resultsPerPage"))
                                    .addQueryParam("direction", "asc");
                            //TODO:: move auth token to header from params
                        })
                        .map(request -> { // execute each request i.e add to queue for processing
                            Future<Buffer> future = Future.future();
                            executor
                                    .execute(request)
                                    .setHandler(async -> { // response handler sets response to future object
                                        if(async.succeeded()){
                                            log.info("Got api response for movieTitle : {}", movieTitle);
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

    public Future<JsonObject> getVideoMetadata(List<Buffer> listBuffer, String title) {
        Future<JsonObject> future = Future.future();

        vertx.executeBlocking(
                promise -> { // blocking API handler
                    log.info("extracting video metadata for : {}", title);
                    int totalComments = 0, totalLikes =0, totalViews =0;
                    for(Buffer buffer: listBuffer){
                        JsonArray jsonArray = buffer.toJsonObject().getJsonArray("data");
                        if(jsonArray == null) continue;

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
                                    log.debug("viewsStr : {}", viewsStr);
                                    if(viewsStr != null) {
                                        views = Integer.valueOf(viewsStr);
                                        totalViews += views;
                                    }
                                }

                                String uri = videoData.getString("uri");
                                log.debug("uri : {}, totalComments : {} , totalLikes : {}, views : {}", uri, comments, likes, views);
                            }
                        }
                    }

                    JsonObject videoStat = new JsonObject()
                            .put("title", title)
                            .put("totalComments", totalComments)
                            .put("totalLikes", totalLikes)
                            .put("totalViews", totalViews)
                            .put("timestamp", Utils.getDateTimeHour());

//                    future.complete(videoStat);
                    promise.complete(videoStat);
                }
                , false // Run parallel, ignore sequence
                , res -> { // Response handler
                    System.out.println("The result is: " + res.result());
                    future.complete((JsonObject)(res.result()));
                });
        return future;
    }

    // composite response handler to add all responses to request to Future<List>
    public Handler<AsyncResult<CompositeFuture>> joinFutures(Future<List<Buffer>> future) {
        log.debug("CompositeFuture async handler");
        return async -> {
            if (async.succeeded()) {
                CompositeFuture compositeFuture = async.result();
                Collection resultBuffers = IntStream
                        .range(0, compositeFuture.size())
                        .mapToObj(compositeFuture::resultAt)
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
