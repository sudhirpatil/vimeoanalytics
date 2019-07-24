package com.sp.parrot;

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

public class VertxRunner extends AbstractVerticle {
    private static final Logger log = LogManager.getLogger(VertxRunner.class);

    ThrottledRequests executor;
    MoviesStore moviesStore;
    VimeoClient vimeoClient;

    // Convenience method so you can run it in your IDE
    public static void main(String[] args) {
        // TODO:: implement fetching https://github.com/vert-x3/vertx-examples/blob/master/web-client-examples/src/main/java/io/vertx/example/webclient/oauth/TwitterOAuthExample.java
        Runner.runExample(VertxRunner.class);
    }

    private void initialize() {
        executor = new ThrottledRequests(vertx);
        moviesStore = new MoviesStore(vertx);
        vimeoClient = new VimeoClient(vertx);
    }

    @Override
    public void start() throws Exception {
        // Get all movie & search term from mysql
            // For each Movie, search all VertxRunner
            // Combine comments, likes , views for each movie
            // Save each movie result to dynamo with movie-id & current hour as key.
            // Save search results to s3
        initialize();

        moviesStore
            .getMovies()
            .compose(this::searchMovies)
            .setHandler(async -> {
                if(async.succeeded()){
                    log.info("movies agg metadata : {}", async.result());
                }else {
                    async.cause().printStackTrace();
                }
                vertx.close();
            });


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

    private CompositeFuture searchMovies(List<JsonObject> movieList) {
        /**
         For each movie
            get future of videos
            get agg metadata of movie
                save to s3
            save to dynamo db
         get list of futures with movie metadata
         create composite future
         */
        List<Future> futureList = movieList.stream().map(movie -> {
            Future<JsonObject> metaFuture = Future.future();
            vimeoClient.searchVideos(executor, movie.getString("movie_title"), 5)
                    .compose(vimeoClient::getVideoMetadata)
                    .setHandler(async -> {
                        if(async.succeeded()){
                            log.info("Movie metadata for {} : {}", movie.getString("movie_id"), async.result());
                            metaFuture.complete(async.result());
                        }else {
                            metaFuture.fail(async.cause());
                        }
                    });
            return metaFuture;
        })
        .collect(Collectors.toList());;

        return CompositeFuture
                .all(futureList);
    }



}
