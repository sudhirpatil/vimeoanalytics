package com.sp.parrot.vertx;

import io.vertx.core.DeploymentOptions;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;

import java.io.File;
import java.io.IOException;
import java.util.function.Consumer;

/*
 * @author <a href="http://tfox.org">Tim Fox</a>
 */
public class Runner {

    private static final String DIR = ".";
    private static final String JAVA_DIR = DIR + "/src/main/java/";

    public static void run(Class clazz) {
        run(JAVA_DIR, clazz, new VertxOptions().setWorkerPoolSize(5).setClustered(false), null);
    }

    public static void run(Class clazz, DeploymentOptions options) {
        run(JAVA_DIR, clazz, new VertxOptions().setClustered(false), options);
    }

    public static void run(String exampleDir, Class clazz, VertxOptions options, DeploymentOptions
            deploymentOptions) {
        run(exampleDir + clazz.getPackage().getName().replace(".", "/"), clazz.getName(), options, deploymentOptions);
    }

    public static void run(String exampleDir, String verticleID, VertxOptions options, DeploymentOptions deploymentOptions) {
        if (options == null) {
            // Default parameter
            options = new VertxOptions();
        }
        // Smart cwd detection

        // Based on the current directory (.) and the desired directory (exampleDir), we try to compute the vertx.cwd
        // directory:
        try {
            // We need to use the canonical file. Without the file name is .
            File current = new File(".").getCanonicalFile();
            if (exampleDir.startsWith(current.getName()) && !exampleDir.equals(current.getName())) {
                exampleDir = exampleDir.substring(current.getName().length() + 1);
            }
        } catch (IOException e) {
            // Ignore it.
        }

        System.setProperty("vertx.cwd", exampleDir);
        Consumer<Vertx> runner = vertx -> {
            try {
                if (deploymentOptions != null) {
                    vertx.deployVerticle(verticleID, deploymentOptions);
                } else {
                    vertx.deployVerticle(verticleID);
                }
            } catch (Throwable t) {
                t.printStackTrace();
            }
        };
        Vertx vertx = Vertx.vertx(options);
        runner.accept(vertx);
    }
}