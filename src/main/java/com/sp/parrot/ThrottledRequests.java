package com.sp.parrot;

import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpClient;
import io.vertx.core.http.HttpClientResponse;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.client.HttpRequest;
import io.vertx.ext.web.client.HttpResponse;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;


public class ThrottledRequests{
    private static final Logger log = LogManager.getLogger(ThrottledRequests.class);

    private static final int RATE_LIMIT_WINDOW_SIZE = 60000;
    private final BlockingQueue<RequestContext> queue = new LinkedBlockingQueue<>();
    private final AtomicLong executorId = new AtomicLong(0);
    private final AtomicLong actualDelay = new AtomicLong(100);
    private final HttpClient http;
    private final Vertx vertx;

    ThrottledRequests(Vertx vertx) {
        this.vertx = vertx;
        this.http = vertx.createHttpClient();
        long id = vertx.setPeriodic(actualDelay.get(), executor());
        this.executorId.set(id);
    }

    public Future<Buffer> execute(HttpRequest<Buffer> request) {
        try {
            Future<Buffer> future = Future.future();
            log.info("adding request to queue");
            queue.put(new RequestContext(request, future));
            return future;
        } catch (Exception e) {
            return Future.failedFuture(e);
        }
    }

//    private Handler<Long> executorOld() {
//        log.info("Instancing new request executor with throttled delay of {} ms", actualDelay.get());
//        return timerId -> {
//            RequestContext context = queue.poll();
//            if (context != null) {
//                Request inputRequest = context.request;
//                log.info("Send request: {} {}", inputRequest.method(), inputRequest.uri());
//                http.request(inputRequest.method(), inputRequest.options())
//                        .putHeader("User-Agent", "Tradegs/0.1")
//                        .setFollowRedirects(true)
//                        .handler(response -> {
//                            response.bodyHandler(context.future::complete);
//                            checkAndUpdateRateLimit(response);
//                        })
//                        .end();
//            }
//        };
//    }

    private Handler<Long> executor() {
        log.info("Instancing new request executor with throttled delay of {} ms", actualDelay.get());
        return timerId -> {
            RequestContext context = queue.poll();
            if (context != null) {
                HttpRequest<Buffer> request = context.request;
                log.info("executor() calling request.send");

                request.send(response -> {
                    log.info("executor() response: {}");
                    context.future.complete(response.result().bodyAsBuffer());
                    checkAndUpdateRateLimit(response.result());
                });
            }
        };
    }

    private void checkAndUpdateRateLimit(HttpResponse<Buffer> response) {
        Optional.ofNullable(response.getHeader("X-RateLimit-Limit"))
            .map(Long::parseLong)
            .map(rateLimit -> rateLimit - 1)
            .map(requestsPerMinute -> RATE_LIMIT_WINDOW_SIZE / requestsPerMinute)
            .ifPresent(throttleDelay -> {
                if (throttleDelay != actualDelay.getAndSet(throttleDelay)) {
                    log.info("Periodic  Scheduler with Delay : {}", throttleDelay);
                    vertx.cancelTimer(executorId.get());
                    long id = vertx.setPeriodic(throttleDelay, executor());
                    executorId.set(id);
                }
            });
    }

    private static class RequestContext {
        private final HttpRequest<Buffer> request;
        private final Future<Buffer> future;

        private RequestContext(HttpRequest<Buffer> request, Future<Buffer> future) {
            this.request = request;
            this.future = future;
        }
    }

}
