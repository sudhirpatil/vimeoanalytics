package com.sp.parrot;

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

public class JobContext {


    Vertx vertx;
    JsonObject config;

    public JobContext(Vertx vertx, String configPath) {
        this.vertx = vertx;
        this.config = config;
    }

    public JobContext(Vertx vertx) {
        this.vertx = vertx;
        this.config = config;
    }

    public Vertx getVertx() {
        return vertx;
    }

    public JsonObject getConfig() {
        return config;
    }
}
