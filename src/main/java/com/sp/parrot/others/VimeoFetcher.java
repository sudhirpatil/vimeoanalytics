package com.sp.parrot.others;

import com.sp.parrot.vertx.Runner;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.client.HttpRequest;
import io.vertx.ext.web.client.HttpResponse;
import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.client.WebClientOptions;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.TimeUnit;

/*
 * @author <a href="mailto:julien@julienviet.com">Julien Viet</a>
 */
public class VimeoFetcher extends AbstractVerticle {
    private static final Logger log = LogManager.getLogger(VimeoFetcher.class);

    WebClient client;
    protected static  String accessToken;

    // Convenience method so you can run it in your IDE
    public static void main(String[] args) {
        accessToken = "87f6e24bf44bb198227ca7a176d90d6b";
        Runner.run(VimeoFetcher.class);
    }

    @Override
    public void start() throws Exception {

        // Create the web client and enable SSL/TLS with a trust store
        client = WebClient.create(vertx,
                new WebClientOptions()
                        .setSsl(true)
        );

        searchVideos( "spider", 1, 1)
                .setHandler(resp -> {
                    if (resp.succeeded()) {
                        log.info("Search videos completed");
                    } else {
                        resp.cause().printStackTrace();
                    }
                });
    }

    public Future<JsonObject> execute(HttpRequest<Buffer> request) {
        try {
            Future<JsonObject> future = Future.future();
            request.send(response -> {
                HttpResponse<Buffer> videoResp = response.result();
                log.info("X-RateLimit-Limit : "+ videoResp.getHeader("X-RateLimit-Limit"));
                log.info("X-RateLimit-Reset : "+ videoResp.getHeader("X-RateLimit-Reset"));
                log.info("X-RateLimit-Remaining : "+ videoResp.getHeader("X-RateLimit-Remaining"));
                log.info("Got HTTP response with status " + videoResp.statusCode());
                if (response.succeeded()) {
                    JsonObject videosJson = videoResp.bodyAsJsonObject();
                    log.info("execute json response: {}",videosJson);
                    future.complete(videosJson);
                } else {
//                        response.cause().printStackTrace();
                    future.fail(response.cause());
                }
            });
            return future;
        } catch (Exception e) {
            return Future.failedFuture(e);
        }
    }

    public Future<JsonObject> searchVideos(String queryTerm, int pageNum, int maxPages ){
        Future<JsonObject> future = Future.future();

        log.info("Searching queryTerm:"+queryTerm+"  page:"+pageNum);
        HttpRequest<Buffer> request = client.get(443, "api.vimeo.com", "/videos")
//                .putHeader("Authorization", "basic "+accessToken)
                .addQueryParam("access_token", accessToken)
                .addQueryParam("query", queryTerm)
                .addQueryParam("page", String.valueOf(pageNum))
                .addQueryParam("per_page", "20")
                .addQueryParam("direction", "asc");
        execute(request).setHandler(async -> {
            if (async.succeeded()) {
                log.info("Search Video received: ", async.result());
                future.complete(async.result());
            } else {
                future.fail(async.cause());
            }
        });
//                .send(response -> {
//                    //TODO:: handle rate limiting
//                    HttpResponse<Buffer> videoResp = response.result();
//                    log.info("X-RateLimit-Limit : "+ videoResp.getHeader("X-RateLimit-Limit"));
//                    log.info("X-RateLimit-Reset : "+ videoResp.getHeader("X-RateLimit-Reset"));
//                    log.info("X-RateLimit-Remaining : "+ videoResp.getHeader("X-RateLimit-Remaining"));
//                    log.info("Got HTTP response with status " + videoResp.statusCode());
//                    if (response.succeeded()) {
//                        JsonObject videosJson = videoResp.bodyAsJsonObject();
//                        log.info("json response: {}",videosJson);
//                        log.info("stats : {}", videosJson.getJsonObject("stats"));
////                        getVideoComments(client, accessToken, 1, videosJson, null);
//                        int pageNumber = pageNum + 1;
//                        if(pageNum <= maxPages){
////                            searchVideos(client, accessToken, queryTerm, pageNumber, maxPages );
//                        }
//                        future.complete();
//                    } else {
////                        response.cause().printStackTrace();
//                        future.fail(response.cause());
//                    }
//                });

        return future;
    }

    public void getVideoComments(WebClient client, String accessToken, int page, JsonObject videosJson, String currentUri){
        JsonArray data = videosJson.getJsonArray("data");
        data.forEach(json -> {
            if(json instanceof JsonObject) {
                JsonObject videoData = (JsonObject) json;
                log.info("Fetching comments for : {}", videoData.getString("uri"));

//                "metadata": {
//                    "connections": {
//                        "comments": {
//                            "uri": "/videos/21037121/comments",
                JsonObject commentObject = videoData.getJsonObject("metadata").getJsonObject("connections").getJsonObject("comments");
                String commentsUri;// = commentObject.getString("uri");
                if(currentUri != null){
                    commentsUri = currentUri;
                }else {
                    commentsUri = String.format("%s?access_token=%s&page=%d&per_page=20&direction=desc",
                            commentObject.getString("uri"), accessToken, page);
                }
                log.info("Current comments uri : {}", currentUri);

                // /videos/342154301/comments?access_token=87f6e24bf44bb198227ca7a176d90d6b&page=1&per_page=26&direction=desc

                client.get(443, "api.vimeo.com", commentsUri)
//                        .addQueryParam("access_token", accessToken)
//                        .addQueryParam("page", "1")
//                        .addQueryParam("per_page", "20")
//                        .addQueryParam("direction", "desc")
                        .send(resonse -> {
                            //TODO:: handle rate limiting
                            HttpResponse<Buffer> commentResponse = resonse.result();
                            int responseCode = commentResponse.statusCode();
                            int remainingLimits = Integer.valueOf(commentResponse.getHeader("X-RateLimit-Remaining"));
                            log.info("commentApi X-RateLimit-Limit : "+ commentResponse.getHeader("X-RateLimit-Limit"));
                            log.info("commentApi X-RateLimit-Reset : "+ commentResponse.getHeader("X-RateLimit-Reset"));
                            log.info("commentApi X-RateLimit-Remaining : {}",remainingLimits);
                            log.info("Got HTTP response with status " + responseCode);
                            JsonObject commentsJson = commentResponse.bodyAsJsonObject();
                            if (resonse.succeeded() && responseCode == 200) {
                                log.info("Response for {} :: {}", commentsUri, commentsJson);
                                String nextUri = commentsJson.getJsonObject("paging").getString("next");
                                if(nextUri != null && nextUri != "null"){
                                    if(remainingLimits > 0){
                                        log.info("Calling nextUri {}, currentUri {}", nextUri, currentUri);
                                        getVideoComments(client, accessToken, 0, videosJson,nextUri);
                                    }else {
                                        log.info("Rate limit finished, scheduling nextUri {} for next minute", nextUri);
                                        vertx.setTimer(TimeUnit.MINUTES.toMillis(1), id -> {
                                            log.info("Scheduled retry from successful call");
//                                            getVideoComments(client, accessToken, 0, videosJson,nextUri);
                                        });
                                    }
                                }
                            } else {
                                log.warn("Failed with status code : {} , json : {}",responseCode, commentsJson);
                                // If failed with "Too many API requests", retry after 1 minute
                                if(responseCode == 429){
                                    log.info("Failed with error code {}, scheduling retry uri {} for next minute", responseCode, currentUri);
                                    vertx.setTimer(TimeUnit.MINUTES.toMillis(1), id -> {
                                        log.info("Scheduled retry");
//                                        getVideoComments(client, accessToken, 0, videosJson,currentUri);
                                    });
                                }
                            }
                        });
            }else {
                log.error("Object in data array is not JsonObjct {}", json);
            }

        });
    }
}
