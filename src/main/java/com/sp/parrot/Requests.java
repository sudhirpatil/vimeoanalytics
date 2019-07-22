package com.sp.parrot;

import io.vertx.core.Future;
import io.vertx.core.buffer.Buffer;

public interface Requests {

    Future<Buffer> execute(Request request);
}
