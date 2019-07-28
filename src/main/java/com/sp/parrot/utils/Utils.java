package com.sp.parrot.utils;

import com.sp.parrot.VertxRunner;
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

import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;

public class Utils {
    private static final Logger log = LogManager.getLogger(Utils.class);
    
    static DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:00:00");
    static DateTimeFormatter dateTimeKey = DateTimeFormatter.ofPattern("yyyy-MM-dd-HH-mm-ss");

    public static String getDateTimeHour(){
        return ZonedDateTime.now(ZoneId.of("UTC")).format(dateTimeFormatter);
    }

    public static String getDateTimeKey(){
        return ZonedDateTime.now(ZoneId.of("UTC")).format(dateTimeKey);
    }

    public static Future<JsonObject> getConfig(Vertx vertx) {
        Future<JsonObject> future = Future.future();

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
                log.info("Cofig file content: {}", config);
                future.complete(config);
            }
        });

        return future;
    }
}
