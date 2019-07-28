package com.sp.parrot;

import com.sp.parrot.api.VimeoApiClient;
import com.sp.parrot.stores.DynamoClient;
import com.sp.parrot.stores.JdbcClient;
import com.sp.parrot.stores.S3ClientAsync;
import com.sp.parrot.utils.Utils;
import com.sp.parrot.vertx.Runner;
import com.sp.parrot.vimeo.VimeoClient;
import io.vertx.core.*;
import io.vertx.core.json.JsonObject;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.stream.Collectors;

public class VertxRunner extends AbstractVerticle {
    private static final Logger log = LogManager.getLogger(VertxRunner.class);

    JsonObject config;
    VimeoApiClient executor;
    JdbcClient jdbcClient;
    VimeoClient vimeoClient;
    DynamoClient dynamoClient;
    S3ClientAsync s3ClientAsync;

    // Convenience method so you can run it in your IDE
    public static void main(String[] args) {
        // TODO:: implement access token fetching
        Runner.run(VertxRunner.class);
    }

    private Future<Void> initialize() {
        Future<Void> future = Future.future();
        Utils.getConfig(vertx).setHandler(async -> {
           if(async.succeeded()){
               this.config = async.result();

               executor = new VimeoApiClient(vertx, config);
               jdbcClient = new JdbcClient(vertx, config);
               vimeoClient = new VimeoClient(vertx, config);
               dynamoClient = new DynamoClient(vertx, config);
               s3ClientAsync = new S3ClientAsync(vertx, config);
               future.complete();
           }else {
                log.error("Failed to read configuration file : {}", async.cause());
                future.fail(async.cause());
           }
        });
        return future;
    }

    @Override
    public void start() throws Exception {
        // Get all movie & search term from mysql
            // For each Movie, search all VertxRunner
            // Combine comments, likes , views for each movie
            // Save each movie result to dynamo with movie-id & current hour as key.
            // Save search results to s3

//        vertx.setPeriodic(actualDelay.get(), executor());

        //TODO:: schedule search every hour
        initialize()
            .compose(config -> jdbcClient.getMovies())
            .compose(this::searchNSaveMovies)
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

    private CompositeFuture searchNSaveMovies(List<JsonObject> movieList) {
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
                .map(movie -> {
                    log.info("movie: {}", movie.toString());
                    String title = movie.getString("movie_title");
                    String s3Key = movie.getString("movie_id") + "_" + Utils.getDateTimeKey();

                    Future<Void> videosJson = vimeoClient
                            .searchVideos(executor, title, 5)
                            .compose(videos -> s3ClientAsync.save(config.getString("awss3bucket"), s3Key, videos))
                            .compose(videos -> vimeoClient.getVideoMetadata(videos, title))
                            .compose(videoMeta -> dynamoClient.saveMovieAsync(config.getString("dynamoTable"), videoMeta));
                    return videosJson;
                })
                .collect(Collectors.toList());

        return CompositeFuture
                .all(futureList);
    }



}
