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
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class VertxRunner extends AbstractVerticle {
    private static final Logger log = LogManager.getLogger(VertxRunner.class);

    ThrottledRequests executor;
    MoviesStore moviesStore;
    VimeoClient vimeoClient;
    DynamoClient dynamoClient;
    S3ClientAsync s3ClientAsync;

    // Convenience method so you can run it in your IDE
    public static void main(String[] args) {
        // TODO:: implement fetching https://github.com/vert-x3/vertx-examples/blob/master/web-client-examples/src/main/java/io/vertx/example/webclient/oauth/TwitterOAuthExample.java
        Runner.runExample(VertxRunner.class);
    }

    private void initialize() {
        executor = new ThrottledRequests(vertx);
        moviesStore = new MoviesStore(vertx);
        vimeoClient = new VimeoClient(vertx);
        dynamoClient = new DynamoClient(vertx);
        s3ClientAsync = new S3ClientAsync(vertx);
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
                    log.info("Completed searching and saving, movies agg metadata : {}", async.result());
                }else {
                    log.error("Failed to Search and save Movies");
                    async.cause().printStackTrace();
                }
                vertx.close();
            });
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
        List<Future> futureList = movieList.stream()
                .flatMap(movie -> {
                    log.info("movie: {}", movie.toString());
                    String title = movie.getString("movie_title");
                    Future<List<Buffer>> videosJson = vimeoClient
                            .searchVideos(executor, title, 5);

                    Future<Void> dynamoSaveFut =
                    videosJson.compose(videos -> vimeoClient.getVideoMetadata(videos, title))
                            .compose(videoMeta -> dynamoClient.saveMovieAsync("spe-sudhir", videoMeta));

                    String s3Key = movie.getString("movie_id") + "_" + Utils.getDateTimeKey();
                    Future<Void> saveS3Fut = videosJson.compose(videos -> s3ClientAsync.save("techtasks/spe-sudhir", s3Key, videos));

//                            .compose(videoMeta -> dynamoClient.saveMovieAsync("spe-sudhir", videoMeta));
//                            .setHandler(async -> {
//                                if(async.succeeded()){
//                                    log.info("Movie metadata for {} : {}", movie.getString("movie_id"), async.result());
//                                    metaFuture.complete();
//                                }else {
//                                    metaFuture.fail(async.cause());
//                                }
//                            });
                    return Arrays.asList(dynamoSaveFut, saveS3Fut).stream();
                })
                .collect(Collectors.toList());

        return CompositeFuture
                .all(futureList);
    }



}
