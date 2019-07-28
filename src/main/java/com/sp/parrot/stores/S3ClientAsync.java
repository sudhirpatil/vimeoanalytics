package com.sp.parrot.stores;

import com.hubrick.vertx.s3.client.S3Client;
import com.hubrick.vertx.s3.client.S3ClientOptions;
import com.hubrick.vertx.s3.model.request.PutObjectRequest;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;

public class S3ClientAsync {

    private static final Logger log = LogManager.getLogger(S3ClientAsync.class);

    Vertx vertx;
    S3ClientOptions clientOptions;
    S3Client s3Client;
    JsonObject config;

    public S3ClientAsync(Vertx vertx, JsonObject config){
        this.vertx = vertx;
        this.config = config;

        S3ClientOptions clientOptions = new S3ClientOptions()
                .setAwsRegion(config.getString("awsRegion"))
                .setAwsServiceName("s3")
                .setAwsAccessKey(config.getString("awsAccess"))
                .setAwsSecretKey(config.getString("awsSecret"));
        s3Client = new S3Client(vertx, clientOptions);
    }

    public Future<List<Buffer>> save(String bucket, String key, List<Buffer> listBuffer){
        Future<List<Buffer>> future = Future.future();

        JsonArray jsonArray = new JsonArray();
        for(Buffer buffer: listBuffer){
            jsonArray.add(buffer.toJsonObject());
        }

        s3Client.putObject(
            bucket,
            key,
            new PutObjectRequest(jsonArray.toBuffer()).withContentType("application/json"),
            response -> {
                future.complete(listBuffer);
                log.info("Saved to S3, key: {}", key);
            },
            throwable -> future.fail(throwable)
        );
        return future;
    }

    public static void testSave(Vertx vertx){

        //.setSignPayload(true);
//        s3Client.getBucket(
//                "techtasks/spe-sudhir",
//                new GetBucketRequest().withPrefix("prefix"),
//                response -> System.out.println("Response from AWS: " + response.getData().getName()),
//                Throwable::printStackTrace
//        );

//        s3Client.getObject(
//                "techtasks/spe-sudhir",
//                "test",
//                new GetObjectRequest().withResponseContentType("application/json"),
//                response -> {
//                    ReadStream<Buffer> stream= response.getData();
//                    stream.handler(async -> System.out.println(async.toJsonObject()));
//                    System.out.println("Response from AWS: " + response.getHeader().getContentType());
//                },
//                Throwable::printStackTrace
//        );
    }

    public static void main(String[] args) {

    }
}
