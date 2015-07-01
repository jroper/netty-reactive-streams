package com.typesafe.netty.http;

import com.typesafe.netty.HandlerPublisher;
import com.typesafe.netty.HandlerSubscriber;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.http.*;
import io.netty.util.ReferenceCountUtil;
import org.reactivestreams.Publisher;

import java.util.LinkedList;
import java.util.Queue;

import static com.typesafe.netty.LoggingHelper.*;

abstract class HttpStreamsHandler<In extends HttpMessage, Out extends HttpMessage> extends ChannelDuplexHandler {

    private final Queue<Outgoing> outgoing = new LinkedList<>();
    private final Class<In> inClass;
    private final Class<Out> outClass;

    public HttpStreamsHandler(Class<In> inClass, Class<Out> outClass) {
        this.inClass = inClass;
        this.outClass = outClass;
    }

    private boolean ignoreBodyRead;
    private boolean sendLastHttpContent;

    protected abstract boolean hasBody(In in);

    protected abstract In createEmptyMessage(In in);

    protected abstract In createStreamedMessage(In in, Publisher<HttpContent> stream);

    protected void receivedInMessage(ChannelHandlerContext ctx) {}
    protected void consumedInMessage(ChannelHandlerContext ctx) {}
    protected void receivedOutMessage(ChannelHandlerContext ctx) {}
    protected void sentOutMessage(ChannelHandlerContext ctx) {}

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        logIn(ctx, "Read " + msg.toString().split("\n")[0]);
        if (inClass.isInstance(msg)) {

            receivedInMessage(ctx);
            In inMsg = inClass.cast(msg);

            if (inMsg instanceof FullHttpMessage) {

                logIn(ctx, "Full body");

                // Forward as is
                ctx.fireChannelRead(inMsg);
                consumedInMessage(ctx);
            } else if (!hasBody(inMsg)) {

                logIn(ctx, "No body");

                // Wrap in empty message
                ctx.fireChannelRead(createEmptyMessage(inMsg));
                consumedInMessage(ctx);

                // There will be a LastHttpContent message coming after this, ignore it
                ignoreBodyRead = true;

            } else {

                if (inMsg instanceof HttpRequest) {
                    logIn(ctx, "Streamed body " + ((HttpRequest) inMsg).getUri());
                }
                // It has a body, stream it
                HandlerPublisher<HttpContent> publisher = new HandlerPublisher<>(HttpContent.class);
                ctx.channel().pipeline().addAfter(ctx.name(), ctx.name() + "-body-publisher", publisher);

                ctx.fireChannelRead(createStreamedMessage(inMsg, publisher));
            }
        } else if (msg instanceof HttpContent) {
            handleReadHttpContent(ctx, (HttpContent) msg);
        }
    }


    private void handleReadHttpContent(ChannelHandlerContext ctx, HttpContent content) {
        if (!ignoreBodyRead) {

            if (content instanceof LastHttpContent) {

                if (content.content().readableBytes() > 0 ||
                        !((LastHttpContent) content).trailingHeaders().isEmpty()) {
                    // It has data or trailing headers, send them
                    ctx.fireChannelRead(content);
                } else {
                    ReferenceCountUtil.release(content);
                }

                ctx.fireChannelInactive();
                ctx.channel().pipeline().remove(ctx.name() + "-body-publisher");
                consumedInMessage(ctx);
                ctx.fireChannelReadComplete();

            } else {
                ctx.fireChannelRead(content);
            }

        } else {
            ReferenceCountUtil.release(content);
            if (content instanceof LastHttpContent) {
                ignoreBodyRead = false;
            }
        }
    }

    @Override
    public void read(ChannelHandlerContext ctx) throws Exception {
        logIn(ctx, "reading");
        super.read(ctx);
    }

    @Override
    public void channelReadComplete(ChannelHandlerContext ctx) throws Exception {
        logIn(ctx, "read complete");
        if (ignoreBodyRead) {
            ctx.read();
        } else {
            ctx.fireChannelReadComplete();
        }
    }

    @Override
    public void write(final ChannelHandlerContext ctx, Object msg, final ChannelPromise promise) throws Exception {
        if (outClass.isInstance(msg)) {

            Outgoing out = new Outgoing(outClass.cast(msg), promise);
            receivedOutMessage(ctx);

            if (outgoing.isEmpty()) {
                outgoing.add(out);
                flushNext(ctx);
            } else {
                logOut(ctx, "Buffering HTTP message");
                outgoing.add(out);
            }

        } else if (msg instanceof LastHttpContent) {
            logOut(ctx, "Write " + msg);
            sendLastHttpContent = false;
            ctx.write(msg, promise);
        } else {
            logOut(ctx, "Write " + msg);
            ctx.write(msg, promise);
        }
    }

    private void unbufferedWrite(final ChannelHandlerContext ctx, final Outgoing out) {
        logOut(ctx, "Write " + out.message.toString().split("\n")[0]);

        if (out.message instanceof FullHttpMessage) {
            // Forward as is
            ctx.writeAndFlush(out.message, out.promise);
            outgoing.remove();
            flushNext(ctx);

        } else if (out.message instanceof StreamedHttpMessage) {

            StreamedHttpMessage streamed = (StreamedHttpMessage) out.message;

            HandlerSubscriber<HttpContent> subscriber = new HandlerSubscriber<HttpContent>() {
                @Override
                protected void error(Throwable error) {
                    out.promise.tryFailure(error);
                    ctx.close();
                }

                @Override
                protected void complete() {
                    if (ctx.executor().inEventLoop()) {
                        logOut(ctx, "SUB complete in event loop");
                        completeBody(ctx);
                    } else {
                        logOut(ctx, "SUB async");
                        ctx.executor().execute(new Runnable() {
                            @Override
                            public void run() {
                                completeBody(ctx);
                            }
                        });
                    }
                }
            };

            sendLastHttpContent = true;

            // DON'T pass the promise through, create a new promise instead.
            ctx.write(out.message);

            ctx.pipeline().addAfter(ctx.name(), ctx.name() + "-body-subscriber", subscriber);
            streamed.subscribe(subscriber);
        }

    }

    private void completeBody(ChannelHandlerContext ctx) {
        ctx.pipeline().remove(ctx.name() + "-body-subscriber");

        logOut(ctx, "complete body");

        ChannelPromise promise = outgoing.remove().promise;
        if (sendLastHttpContent) {
            logOut(ctx, "Writing last content");
            ctx.writeAndFlush(LastHttpContent.EMPTY_LAST_CONTENT, promise);
        } else {
            promise.setSuccess();
        }

        flushNext(ctx);
    }

    private void flushNext(ChannelHandlerContext ctx) {
        sentOutMessage(ctx);
        if (!outgoing.isEmpty()) {
            unbufferedWrite(ctx, outgoing.element());
        } else {
            ctx.fireChannelWritabilityChanged();
        }
    }

    private class Outgoing {
        final Out message;
        final ChannelPromise promise;

        public Outgoing(Out message, ChannelPromise promise) {
            this.message = message;
            this.promise = promise;
        }
    }
}
