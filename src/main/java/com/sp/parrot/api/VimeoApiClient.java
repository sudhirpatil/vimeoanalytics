package com.sp.parrot.api;

import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;

public class VimeoApiClient extends ThrottledApiClient{
    JsonObject config;
    public VimeoApiClient(Vertx vertx, JsonObject config) {
        super(vertx);
        this.config = config;
    }

    public  String getRateLimitHeader(){
        return "X-RateLimit-Limit";
    }
}
