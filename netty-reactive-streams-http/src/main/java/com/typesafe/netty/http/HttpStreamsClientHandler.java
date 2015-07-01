package com.typesafe.netty.http;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.http.*;
import org.reactivestreams.Publisher;

import static com.typesafe.netty.LoggingHelper.*;

/**
 * Takes HTTP requests into single H
 */
public class HttpStreamsClientHandler extends HttpStreamsHandler<HttpResponse, HttpRequest> {

    private int inFlight = 0;
    private ChannelPromise closeOnZeroInFlight = null;

    public HttpStreamsClientHandler() {
        super(HttpResponse.class, HttpRequest.class);
    }

    @Override
    protected boolean hasBody(HttpResponse response) {
        if (response.getStatus().code() >= 100 && response.getStatus().code() < 200) {
            return false;
        }

        if (response.getStatus().equals(HttpResponseStatus.NO_CONTENT) ||
                response.getStatus().equals(HttpResponseStatus.NOT_MODIFIED)) {
            return false;
        }

        if (HttpHeaders.isTransferEncodingChunked(response)) {
            return true;
        }


        if (HttpHeaders.isContentLengthSet(response)) {
            return HttpHeaders.getContentLength(response) > 0;
        }

        return true;
    }

    @Override
    public void close(ChannelHandlerContext ctx, ChannelPromise future) throws Exception {
        if (inFlight == 0) {
            ctx.close(future);
        } else {
            logIn(ctx, "Buffering close");
            closeOnZeroInFlight = future;
        }
    }

    @Override
    protected void consumedInMessage(ChannelHandlerContext ctx) {

        logIn(ctx, "Consumed response " + inFlight);

        inFlight--;

        if (inFlight == 0 && closeOnZeroInFlight != null) {
            logIn(ctx, "Closing immediately");
            ctx.close(closeOnZeroInFlight);
        }
    }

    @Override
    protected void receivedOutMessage(ChannelHandlerContext ctx) {
        inFlight++;
        logOut(ctx, "Received request " + inFlight);
    }

    @Override
    protected HttpResponse createEmptyMessage(HttpResponse response) {
        return new EmptyHttpResponse(response);
    }

    @Override
    protected HttpResponse createStreamedMessage(HttpResponse response, Publisher<HttpContent> stream) {
        return new DelegateStreamedHttpResponse(response, stream);
    }

}
