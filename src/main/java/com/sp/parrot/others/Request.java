package com.sp.parrot.others;

import io.vertx.core.http.HttpMethod;
import io.vertx.core.http.RequestOptions;

public class Request {
    private final HttpMethod method;
    private final RequestOptions options;

    public static Request get(String path) {
        return new Request(HttpMethod.GET, new RequestOptions()
                .setHost("api.discogs.com")
                .setURI(path)
        );
    }

    public static Request get(RequestOptions options) {
        return new Request(HttpMethod.GET, options);
    }

    public Request(HttpMethod method, RequestOptions options) {
        this.method = method;
        this.options = options;
    }

    public HttpMethod method() {
        return method;
    }

    public RequestOptions options() {
        return options;
    }

//    @Override
//    public String toString() {
//        return ToStringBuilder.reflectionToString(this);
//    }

    public String uri() {
        return options.getURI();
    }
}
