package com.typesafe.netty;

import io.netty.channel.ChannelHandlerContext;

import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;

public class LoggingHelper {

    private static Writer clientIn;
    private static Writer serverIn;
    private static Writer clientOut;
    private static Writer serverOut;

    private static long startTime;

    private static String time() {
        return Long.toString(System.currentTimeMillis() - startTime);
    }

    public static void start() throws Exception {
        clientIn = new FileWriter("/tmp/clientIn.log");
        serverIn = new FileWriter("/tmp/serverIn.log");
        clientOut = new FileWriter("/tmp/clientOut.log");
        serverOut = new FileWriter("/tmp/serverOut.log");
        startTime = System.currentTimeMillis();
    }

    public static void stop() throws Exception {
        clientIn.close();
        serverIn.close();
        clientOut.close();
        serverOut.close();
    }

    public static void logIn(ChannelHandlerContext ctx, String message) {
        logIn(ctx == null ? "null" : ctx.name(), message);
    }

    public static void logIn(String name, String message) {
        try {
            if (name.startsWith("client")) {
                clientIn.write(time() + " " + name + ": " + message + "\n");
            } else {
                serverIn.write(time() + " " + name + ": " + message + "\n");
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static void logOut(ChannelHandlerContext ctx, String message) {
        logOut(ctx == null ? "null" : ctx.name(), message);
    }

    public static void logOut(String name, String message) {
        try {
            if (name.startsWith("client")) {
                clientOut.write(time() + " " + name + ": " + message + "\n");
            } else {
                serverOut.write(time() + " " + name + ": " + message + "\n");
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

}
