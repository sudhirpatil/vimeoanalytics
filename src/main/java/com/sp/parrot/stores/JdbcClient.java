package com.sp.parrot.stores;

import io.vertx.config.ConfigRetriever;
import io.vertx.config.ConfigRetrieverOptions;
import io.vertx.config.ConfigStoreOptions;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.asyncsql.MySQLClient;
import io.vertx.ext.sql.ResultSet;
import io.vertx.ext.sql.SQLClient;
import io.vertx.ext.sql.SQLConnection;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;

public class JdbcClient {
    private static final Logger log = LogManager.getLogger(JdbcClient.class);

    Vertx vertx;
    JsonObject config;

    public JdbcClient(Vertx vertx, JsonObject config){
        this.vertx = vertx;
        this.config = config;
    }
    /**
     * Fetch movies from mysql
     * @return
     */
    public Future<List<JsonObject>> getMovies() {
        Future<List<JsonObject>> future = Future.future();

        // load config file
        ConfigRetriever retriever = ConfigRetriever
                .create(vertx, new ConfigRetrieverOptions()
                        .addStore(new ConfigStoreOptions().setType("file").
                                setConfig(new JsonObject().put("path", "./../../../../../../conf/config.json"))));

        // Connect to db as per config and fetch from movies table
        retriever.getConfig(ar -> {
            if (ar.failed()) {
                future.fail(ar.cause());
            } else {
                JsonObject config = ar.result();
                log.info("Cofig file content: {}",config);
                log.info("creating mysql connection");
                SQLClient mySQLClient = MySQLClient.createShared(vertx, config);
                mySQLClient.getConnection(res -> {
                    if (res.succeeded()) {
                        log.info("mysql connection created");
                        SQLConnection connection = res.result();
                        connection.query("SELECT * FROM search_term_movies", result -> {
                            ResultSet resultSet = result.result();
//                            log.info("getRows {}", resultSet.getRows());
                            log.info("Got number of rows from mysql table : {}",result.result().getNumRows());
                            log.info("Column names : {}",result.result().getColumnNames());
                            future.complete(resultSet.getRows());
                            connection.close(done -> {
                                log.info("Closed db connection");
                            });
                        });
                    } else {
                        future.fail(res.cause());
                    }
                });
            }
        });

        return future;
    }
}
