package com.typesafe.netty;

import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.local.LocalChannel;
import org.reactivestreams.Publisher;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Factory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class HandlerPublisherVerificationTest extends AbstractHandlerPublisherVerification {

    private final int batchSize;
    // The number of elements to publish initially, before the subscriber is received
    private final int publishInitial;
    // Whether the end of stream should be triggered by a stream close, or by the message handler
    private final boolean close;

    @Factory(dataProvider = "dp")
    public HandlerPublisherVerificationTest(int batchSize, int publishInitial, boolean close) {
        this.batchSize = batchSize;
        this.publishInitial = publishInitial;
        this.close = close;
    }

    @DataProvider
    public static Object[][] dp() {
        List<Object[]> data = new ArrayList<>();
        for (Boolean close : Arrays.asList(false)) {
            for (int batchSize : Arrays.asList(1)) {
                for (int publishInitial : Arrays.asList(5)) {
                    data.add(new Object[]{batchSize, publishInitial, close});
                }
            }
        }
        return data.toArray(new Object[][]{});
    }

    @Override
    public Publisher<Long> createPublisher(final long elements) {
        long handlerCompleteOn = elements;
        if (close) {
            handlerCompleteOn = Long.MAX_VALUE;
        }

        final HandlerPublisher<Long> publisher = new HandlerPublisher<>(new BoundedMessageHandler(handlerCompleteOn));

        long eofOn = Long.MAX_VALUE;
        if (close) {
            eofOn = elements;
        }

        final BatchedProducer out = new BatchedProducer()
                .sequence(publishInitial)
                .batchSize(batchSize)
                .eofOn(eofOn);

        final LocalChannel channel = new LocalChannel();
        eventLoop.register(channel).addListener(new ChannelFutureListener() {
            @Override
            public void operationComplete(ChannelFuture future) throws Exception {
                channel.pipeline().addLast("out", out);
                channel.pipeline().addLast("publisher", publisher);

                for (long i = 0; i < publishInitial && i < elements; i++) {
                    channel.pipeline().fireChannelRead(i);
                }
                if (elements <= publishInitial) {
                    channel.pipeline().fireChannelInactive();
                }
            }
        });

        return publisher;
    }
}
