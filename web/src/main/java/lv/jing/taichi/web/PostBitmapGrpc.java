package lv.jing.taichi.web;

import static io.grpc.stub.ClientCalls.asyncUnaryCall;
import static io.grpc.stub.ClientCalls.asyncServerStreamingCall;
import static io.grpc.stub.ClientCalls.asyncClientStreamingCall;
import static io.grpc.stub.ClientCalls.asyncBidiStreamingCall;
import static io.grpc.stub.ClientCalls.blockingUnaryCall;
import static io.grpc.stub.ClientCalls.blockingServerStreamingCall;
import static io.grpc.stub.ClientCalls.futureUnaryCall;
import static io.grpc.MethodDescriptor.generateFullMethodName;
import static io.grpc.stub.ServerCalls.asyncUnaryCall;
import static io.grpc.stub.ServerCalls.asyncServerStreamingCall;
import static io.grpc.stub.ServerCalls.asyncClientStreamingCall;
import static io.grpc.stub.ServerCalls.asyncBidiStreamingCall;
import static io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall;
import static io.grpc.stub.ServerCalls.asyncUnimplementedStreamingCall;

/**
 */
@javax.annotation.Generated(
    value = "by gRPC proto compiler (version 1.1.2)",
    comments = "Source: bmservice.proto")
public class PostBitmapGrpc {

  private PostBitmapGrpc() {}

  private static <T> io.grpc.stub.StreamObserver<T> toObserver(final io.vertx.core.Handler<io.vertx.core.AsyncResult<T>> handler) {
    return new io.grpc.stub.StreamObserver<T>() {
      private volatile boolean resolved = false;
      @Override
      public void onNext(T value) {
        if (!resolved) {
          resolved = true;
          handler.handle(io.vertx.core.Future.succeededFuture(value));
        }
      }

      @Override
      public void onError(Throwable t) {
        if (!resolved) {
          resolved = true;
          handler.handle(io.vertx.core.Future.failedFuture(t));
        }
      }

      @Override
      public void onCompleted() {
        if (!resolved) {
          resolved = true;
          handler.handle(io.vertx.core.Future.succeededFuture());
        }
      }
    };
  }

  public static final String SERVICE_NAME = "RPCBitmap.PostBitmap";

  // Static method descriptors that strictly reflect the proto.
  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  public static final io.grpc.MethodDescriptor<lv.jing.taichi.web.RPCBitmap.BitmapRequest,
      lv.jing.taichi.web.RPCBitmap.BitmapReply> METHOD_GET_BITMAP =
      io.grpc.MethodDescriptor.create(
          io.grpc.MethodDescriptor.MethodType.UNARY,
          generateFullMethodName(
              "RPCBitmap.PostBitmap", "getBitmap"),
          io.grpc.protobuf.ProtoUtils.marshaller(lv.jing.taichi.web.RPCBitmap.BitmapRequest.getDefaultInstance()),
          io.grpc.protobuf.ProtoUtils.marshaller(lv.jing.taichi.web.RPCBitmap.BitmapReply.getDefaultInstance()));

  /**
   * Creates a new async stub that supports all call types for the service
   */
  public static PostBitmapStub newStub(io.grpc.Channel channel) {
    return new PostBitmapStub(channel);
  }

  /**
   * Creates a new blocking-style stub that supports unary and streaming output calls on the service
   */
  public static PostBitmapBlockingStub newBlockingStub(
      io.grpc.Channel channel) {
    return new PostBitmapBlockingStub(channel);
  }

  /**
   * Creates a new ListenableFuture-style stub that supports unary and streaming output calls on the service
   */
  public static PostBitmapFutureStub newFutureStub(
      io.grpc.Channel channel) {
    return new PostBitmapFutureStub(channel);
  }

  /**
   * Creates a new vertx stub that supports all call types for the service
   */
  public static PostBitmapVertxStub newVertxStub(io.grpc.Channel channel) {
    return new PostBitmapVertxStub(channel);
  }

  /**
   */
  public static abstract class PostBitmapImplBase implements io.grpc.BindableService {

    /**
     */
    public void getBitmap(lv.jing.taichi.web.RPCBitmap.BitmapRequest request,
        io.grpc.stub.StreamObserver<lv.jing.taichi.web.RPCBitmap.BitmapReply> responseObserver) {
      asyncUnimplementedUnaryCall(METHOD_GET_BITMAP, responseObserver);
    }

    @java.lang.Override public final io.grpc.ServerServiceDefinition bindService() {
      return io.grpc.ServerServiceDefinition.builder(getServiceDescriptor())
          .addMethod(
            METHOD_GET_BITMAP,
            asyncUnaryCall(
              new MethodHandlers<
                lv.jing.taichi.web.RPCBitmap.BitmapRequest,
                lv.jing.taichi.web.RPCBitmap.BitmapReply>(
                  this, METHODID_GET_BITMAP)))
          .build();
    }
  }

  /**
   */
  public static final class PostBitmapStub extends io.grpc.stub.AbstractStub<PostBitmapStub> {
    private PostBitmapStub(io.grpc.Channel channel) {
      super(channel);
    }

    private PostBitmapStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected PostBitmapStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new PostBitmapStub(channel, callOptions);
    }

    /**
     */
    public void getBitmap(lv.jing.taichi.web.RPCBitmap.BitmapRequest request,
        io.grpc.stub.StreamObserver<lv.jing.taichi.web.RPCBitmap.BitmapReply> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(METHOD_GET_BITMAP, getCallOptions()), request, responseObserver);
    }
  }

  /**
   */
  public static final class PostBitmapBlockingStub extends io.grpc.stub.AbstractStub<PostBitmapBlockingStub> {
    private PostBitmapBlockingStub(io.grpc.Channel channel) {
      super(channel);
    }

    private PostBitmapBlockingStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected PostBitmapBlockingStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new PostBitmapBlockingStub(channel, callOptions);
    }

    /**
     */
    public lv.jing.taichi.web.RPCBitmap.BitmapReply getBitmap(lv.jing.taichi.web.RPCBitmap.BitmapRequest request) {
      return blockingUnaryCall(
          getChannel(), METHOD_GET_BITMAP, getCallOptions(), request);
    }
  }

  /**
   */
  public static final class PostBitmapFutureStub extends io.grpc.stub.AbstractStub<PostBitmapFutureStub> {
    private PostBitmapFutureStub(io.grpc.Channel channel) {
      super(channel);
    }

    private PostBitmapFutureStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected PostBitmapFutureStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new PostBitmapFutureStub(channel, callOptions);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<lv.jing.taichi.web.RPCBitmap.BitmapReply> getBitmap(
        lv.jing.taichi.web.RPCBitmap.BitmapRequest request) {
      return futureUnaryCall(
          getChannel().newCall(METHOD_GET_BITMAP, getCallOptions()), request);
    }
  }

  /**
   */
  public static abstract class PostBitmapVertxImplBase implements io.grpc.BindableService {

    /**
     */
    public void getBitmap(lv.jing.taichi.web.RPCBitmap.BitmapRequest request,
        io.vertx.core.Future<lv.jing.taichi.web.RPCBitmap.BitmapReply> response) {
      asyncUnimplementedUnaryCall(METHOD_GET_BITMAP, PostBitmapGrpc.toObserver(response.completer()));
    }

    @java.lang.Override public final io.grpc.ServerServiceDefinition bindService() {
      return io.grpc.ServerServiceDefinition.builder(getServiceDescriptor())
          .addMethod(
            METHOD_GET_BITMAP,
            asyncUnaryCall(
              new VertxMethodHandlers<
                lv.jing.taichi.web.RPCBitmap.BitmapRequest,
                lv.jing.taichi.web.RPCBitmap.BitmapReply>(
                  this, METHODID_GET_BITMAP)))
          .build();
    }
  }

  /**
   */
  public static final class PostBitmapVertxStub extends io.grpc.stub.AbstractStub<PostBitmapVertxStub> {
    private PostBitmapVertxStub(io.grpc.Channel channel) {
      super(channel);
    }

    private PostBitmapVertxStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected PostBitmapVertxStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new PostBitmapVertxStub(channel, callOptions);
    }

    /**
     */
    public void getBitmap(lv.jing.taichi.web.RPCBitmap.BitmapRequest request,
        io.vertx.core.Handler<io.vertx.core.AsyncResult<lv.jing.taichi.web.RPCBitmap.BitmapReply>> response) {
      asyncUnaryCall(
          getChannel().newCall(METHOD_GET_BITMAP, getCallOptions()), request, PostBitmapGrpc.toObserver(response));
    }
  }

  private static final int METHODID_GET_BITMAP = 0;

  private static class MethodHandlers<Req, Resp> implements
      io.grpc.stub.ServerCalls.UnaryMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ServerStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ClientStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.BidiStreamingMethod<Req, Resp> {
    private final PostBitmapImplBase serviceImpl;
    private final int methodId;

    public MethodHandlers(PostBitmapImplBase serviceImpl, int methodId) {
      this.serviceImpl = serviceImpl;
      this.methodId = methodId;
    }

    @java.lang.Override
    @java.lang.SuppressWarnings("unchecked")
    public void invoke(Req request, io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        case METHODID_GET_BITMAP:
          serviceImpl.getBitmap((lv.jing.taichi.web.RPCBitmap.BitmapRequest) request,
              (io.grpc.stub.StreamObserver<lv.jing.taichi.web.RPCBitmap.BitmapReply>) responseObserver);
          break;
        default:
          throw new AssertionError();
      }
    }

    @java.lang.Override
    @java.lang.SuppressWarnings("unchecked")
    public io.grpc.stub.StreamObserver<Req> invoke(
        io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        default:
          throw new AssertionError();
      }
    }
  }

  private static class VertxMethodHandlers<Req, Resp> implements
      io.grpc.stub.ServerCalls.UnaryMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ServerStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ClientStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.BidiStreamingMethod<Req, Resp> {
    private final PostBitmapVertxImplBase serviceImpl;
    private final int methodId;

    public VertxMethodHandlers(PostBitmapVertxImplBase serviceImpl, int methodId) {
      this.serviceImpl = serviceImpl;
      this.methodId = methodId;
    }

    @java.lang.Override
    @java.lang.SuppressWarnings("unchecked")
    public void invoke(Req request, io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        case METHODID_GET_BITMAP:
          serviceImpl.getBitmap((lv.jing.taichi.web.RPCBitmap.BitmapRequest) request,
              (io.vertx.core.Future<lv.jing.taichi.web.RPCBitmap.BitmapReply>) io.vertx.core.Future.<lv.jing.taichi.web.RPCBitmap.BitmapReply>future().setHandler(ar -> {
                if (ar.succeeded()) {
                  ((io.grpc.stub.StreamObserver<lv.jing.taichi.web.RPCBitmap.BitmapReply>) responseObserver).onNext(ar.result());
                  responseObserver.onCompleted();
                } else {
                  responseObserver.onError(ar.cause());
                }
              }));
          break;
        default:
          throw new AssertionError();
      }
    }

    @java.lang.Override
    @java.lang.SuppressWarnings("unchecked")
    public io.grpc.stub.StreamObserver<Req> invoke(
        io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        default:
          throw new AssertionError();
      }
    }
  }

  private static final class PostBitmapDescriptorSupplier implements io.grpc.protobuf.ProtoFileDescriptorSupplier {
    @java.lang.Override
    public com.google.protobuf.Descriptors.FileDescriptor getFileDescriptor() {
      return lv.jing.taichi.web.RPCBitmap.getDescriptor();
    }
  }

  private static volatile io.grpc.ServiceDescriptor serviceDescriptor;

  public static io.grpc.ServiceDescriptor getServiceDescriptor() {
    io.grpc.ServiceDescriptor result = serviceDescriptor;
    if (result == null) {
      synchronized (PostBitmapGrpc.class) {
        result = serviceDescriptor;
        if (result == null) {
          serviceDescriptor = result = io.grpc.ServiceDescriptor.newBuilder(SERVICE_NAME)
              .setSchemaDescriptor(new PostBitmapDescriptorSupplier())
              .addMethod(METHOD_GET_BITMAP)
              .build();
        }
      }
    }
    return result;
  }
}
