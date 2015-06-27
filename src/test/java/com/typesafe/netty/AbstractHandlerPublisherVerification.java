package com.typesafe.netty;

import io.netty.channel.local.LocalChannel;
import io.netty.channel.local.LocalEventLoopGroup;
import org.reactivestreams.Publisher;
import org.reactivestreams.tck.PublisherVerification;
import org.reactivestreams.tck.TestEnvironment;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;

abstract class AbstractHandlerPublisherVerification extends PublisherVerification<Long> {

    protected LocalEventLoopGroup eventLoop;

    public AbstractHandlerPublisherVerification() {
        super(new TestEnvironment());
    }

    @BeforeClass
    public void startEventLoop() {
        
        eventLoop = new LocalEventLoopGroup();
    }

    @AfterClass
    public void stopEventLoop() {
        eventLoop.shutdownGracefully();
        eventLoop = null;
    }

    @Override
    public Publisher<Long> createFailedPublisher() {
        HandlerPublisher<Long> publisher = new HandlerPublisher<>(new BoundedMessageHandler(0));
        LocalChannel channel = new LocalChannel();
        eventLoop.register(channel);
        channel.pipeline().addLast("publisher", publisher);
        channel.pipeline().fireExceptionCaught(new RuntimeException("failed"));

        return publisher;
    }
}
