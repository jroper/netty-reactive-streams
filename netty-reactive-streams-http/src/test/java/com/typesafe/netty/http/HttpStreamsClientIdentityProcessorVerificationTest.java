package com.typesafe.netty.http;

import akka.actor.ActorSystem;
import akka.dispatch.Futures;
import akka.dispatch.Mapper;
import akka.japi.Pair;
import akka.japi.function.Function;
import akka.japi.function.Function2;
import akka.stream.ActorMaterializer;
import akka.stream.Materializer;
import akka.stream.javadsl.Keep;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import com.typesafe.netty.HandlerPublisher;
import com.typesafe.netty.HandlerSubscriber;
import com.typesafe.netty.LoggingHelper;
import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.Unpooled;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.http.*;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.concurrent.DefaultPromise;
import io.netty.util.concurrent.GenericFutureListener;
import io.netty.util.concurrent.Promise;
import org.reactivestreams.Processor;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.reactivestreams.tck.IdentityProcessorVerification;
import org.reactivestreams.tck.TestEnvironment;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import scala.concurrent.Future;
import scala.runtime.BoxedUnit;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

public class HttpStreamsClientIdentityProcessorVerificationTest extends IdentityProcessorVerification<String> {

    private NioEventLoopGroup eventLoop;
    private Channel serverBindChannel;
    private ActorSystem actorSystem;
    private Materializer materializer;
    private ExecutorService executorService;

    public HttpStreamsClientIdentityProcessorVerificationTest() {
        super(new TestEnvironment(400));
    }

    @BeforeMethod
    public void startServer() throws Exception {
        LoggingHelper.start();
        executorService = Executors.newCachedThreadPool();
        eventLoop = new NioEventLoopGroup();
        ServerBootstrap bootstrap = new ServerBootstrap();
        actorSystem = ActorSystem.create();
        materializer = ActorMaterializer.create(actorSystem);
        bootstrap.group(eventLoop)
                .channel(NioServerSocketChannel.class)
                .childOption(ChannelOption.AUTO_READ, false)
                .localAddress("127.0.0.1", 0)
                .childHandler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) throws Exception {
                        ChannelPipeline pipeline = ch.pipeline();

                        pipeline.addLast(
                                new HttpRequestDecoder(),
                                new HttpResponseEncoder()
                        ).addLast("serverStreamsHandler", new HttpStreamsServerHandler());

                        HandlerSubscriber<HttpResponse> subscriber = new HandlerSubscriber<>(2, 4);
                        HandlerPublisher<HttpRequest> publisher = new HandlerPublisher<>(HttpRequest.class);

                        pipeline.addLast("serverSubscriber", subscriber);
                        pipeline.addLast("serverPublisher", publisher);

                        Source.from(publisher).map(new Function<HttpRequest, HttpResponse>() {
                            public HttpResponse apply(HttpRequest request) throws Exception {
                                HttpResponse response;
                                if (request instanceof StreamedHttpRequest) {
                                    response = new DefaultStreamedHttpResponse(request.getProtocolVersion(),
                                            HttpResponseStatus.OK, (StreamedHttpRequest) request);
                                } else if (request instanceof FullHttpRequest) {
                                    response = new DefaultFullHttpResponse(request.getProtocolVersion(),
                                            HttpResponseStatus.OK, ((FullHttpRequest) request).content());
                                } else {
                                    throw new IllegalArgumentException("Unsupported http message type: " + request);
                                }
                                if (HttpHeaders.isTransferEncodingChunked(request)) {
                                    HttpHeaders.setTransferEncodingChunked(response);
                                } else {
                                    HttpHeaders.setContentLength(response, HttpHeaders.getContentLength(request, 0));
                                }
                                HttpHeaders.setHeader(response, "Location", request.getUri());
                                return response;
                            }
                        }).to(Sink.create(subscriber)).run(materializer);
                    }
                });

        serverBindChannel = bootstrap.bind().await().channel();
    }

    @AfterMethod
    public void stopServer() throws Exception {
        executorService.shutdown();
        serverBindChannel.close().await();
        eventLoop.shutdownGracefully();
        actorSystem.shutdown();
        LoggingHelper.stop();
    }

    @Override
    public Processor<String, String> createIdentityProcessor(int bufferSize) {

        final Promise<Channel> clientChannel = new DefaultPromise<>(eventLoop.next());

        final HandlerPublisher<HttpResponse> publisherHandler = new HandlerPublisher<>(HttpResponse.class);
        final HandlerSubscriber<HttpRequest> subscriberHandler = new HandlerSubscriber<HttpRequest>(2, 4) {
            @Override
            protected void error(final Throwable error) {
                clientChannel.addListener(new GenericFutureListener<io.netty.util.concurrent.Future<Channel>>() {
                    @Override
                    public void operationComplete(io.netty.util.concurrent.Future<Channel> future) throws Exception {
                        ChannelPipeline pipeline = future.getNow().pipeline();
                        pipeline.fireExceptionCaught(error);
                        pipeline.close();
                    }
                });
            }
        };

        Bootstrap client = new Bootstrap()
                .group(eventLoop)
                .option(ChannelOption.AUTO_READ, false)
                .channel(NioSocketChannel.class)
                .handler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) throws Exception {
                        ChannelPipeline pipeline = ch.pipeline();
                        pipeline
                                .addLast(new HttpClientCodec())
                                .addLast("clientStreamsHandler", new HttpStreamsClientHandler())
                                .addLast("clientPublisher", publisherHandler)
                                .addLast("clientSubscriber", subscriberHandler);
                    }
                });

        Publisher<String> publisher = Source.from(publisherHandler).mapAsync(4, new Function<HttpResponse, Future<String>>() {
            @Override
            public Future<String> apply(HttpResponse response) throws Exception {
                if (response instanceof FullHttpResponse) {
                    return Futures.successful(contentAsString((FullHttpResponse) response));
                } else if (response instanceof StreamedHttpResponse) {
                    return Source.from((StreamedHttpResponse) response).runFold("", new Function2<String, HttpContent, String>() {
                        @Override
                        public String apply(String body, HttpContent content) throws Exception {
                            LoggingHelper.logIn("clientActor", "concat chunk");
                            if (body.isEmpty()) {
                                return contentAsString(content);
                            } else {
                                return body + ":" + contentAsString(content);
                            }
                        }
                    }, materializer).map(new Mapper<String, String>() {
                        @Override
                        public String apply(String parameter) {
                            LoggingHelper.logIn("clientActor", "got full body");
                            return parameter;
                        }
                    }, actorSystem.dispatcher());
                } else {
                    throw new IllegalArgumentException("Unknown response type: " + response);
                }
            }
        }).runWith(Sink.<String>publisher(), materializer);

        Pair<Subscriber<String>, Publisher<HttpRequest>> pair = Source.<String>subscriber().map(new Function<String, HttpRequest>() {
            @Override
            public HttpRequest apply(String body) throws Exception {
                String[] chunks = body.split(":");
                final List<HttpContent> content = new ArrayList<>();
                for (String chunk: body.split(":")) {
                    content.add(new DefaultHttpContent(Unpooled.copiedBuffer(chunk, Charset.forName("utf-8"))));
                }

                /*
                Publisher<HttpContent> publisher = new Publisher<HttpContent>() {
                    @Override
                    public void subscribe(final Subscriber<? super HttpContent> s) {
                        s.onSubscribe(new Subscription() {
                            @Override
                            public void request(long n) {
                                for (HttpContent c: content) {
                                    LoggingHelper.logOut("clientActor", "generating chunk");
                                    s.onNext(c);
                                }
                                LoggingHelper.logOut("clientActor", "completing");
                                s.onComplete();
                            }

                            @Override
                            public void cancel() {

                            }
                        });
                    }
                };
                */

                Publisher<HttpContent> publisher = Source.from(content).map(new Function<HttpContent, HttpContent>() {
                    @Override
                    public HttpContent apply(HttpContent param) throws Exception {
                        LoggingHelper.logOut("clientActor", "generating chunk");
                        return param;
                    }
                }).runWith(Sink.<HttpContent>publisher(), materializer);

                HttpRequest request = new DefaultStreamedHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/" + chunks[0],
                        publisher);
                HttpHeaders.setTransferEncodingChunked(request);
                return request;
            }
        }).toMat(Sink.<HttpRequest>publisher(), Keep.<Subscriber<String>, Publisher<HttpRequest>>both()).run(materializer);

        pair.second().subscribe(subscriberHandler);

        client.remoteAddress(serverBindChannel.localAddress());
        client.connect().addListener(new ChannelFutureListener() {
            @Override
            public void operationComplete(ChannelFuture future) throws Exception {
                clientChannel.setSuccess(future.channel());
            }
        });

        return new DelegateProcessor<>(pair.first(), publisher);
    }

    private String contentAsString(HttpContent content) {
        String body = content.content().toString(Charset.forName("utf-8"));
        ReferenceCountUtil.release(content);
        return body;
    }

    @Override
    public Publisher<String> createFailedPublisher() {
        return Source.<String>failed(new RuntimeException("failed"))
                .toMat(Sink.<String>publisher(), Keep.<BoxedUnit, Publisher<String>>right()).run(materializer);
    }

    @Override
    public ExecutorService publisherExecutorService() {
        return executorService;
    }

    @Override
    public String createElement(int element) {
        StringBuilder sb = new StringBuilder();
        sb.append(element);
        for (int i = 0; i < 2; i++) {
            sb.append(":this is a very cool element, it is element number ").append(element);
        }
        return sb.toString();
    }

    @Override
    public long maxSupportedSubscribers() {
        return 1;
    }

}
