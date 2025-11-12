/*
 * Copyright 2025 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.cloud.storage;

import static com.google.cloud.storage.TestUtils.xxd;
import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

import com.google.api.core.SettableApiFuture;
import com.google.api.gax.retrying.BasicResultRetryAlgorithm;
import com.google.api.gax.retrying.ResultRetryAlgorithm;
import com.google.api.gax.rpc.ApiCallContext;
import com.google.api.gax.rpc.ResponseObserver;
import com.google.api.gax.rpc.ServerStreamingCallable;
import com.google.api.gax.rpc.StreamController;
import com.google.cloud.storage.GrpcUtils.ZeroCopyServerStreamingCallable;
import com.google.cloud.storage.Retrying.Retrier;
import com.google.cloud.storage.it.ChecksummedTestContent;
import com.google.protobuf.ByteString;
import com.google.storage.v2.ChecksummedData;
import com.google.storage.v2.ReadObjectRequest;
import com.google.storage.v2.ReadObjectResponse;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

@RunWith(JUnit4.class)
public final class GapicUnbufferedReadableByteChannelTest {

  // A custom, public exception for our test to avoid access issues
  public static class SimulatedTimeoutException extends IOException {
    public SimulatedTimeoutException(String message) {
      super(message);
    }
  }

  @Ignore
  @Test
  public void ensureResponseAreClosed() throws IOException {
    ChecksummedTestContent testContent =
        ChecksummedTestContent.of(DataGenerator.base64Characters().genBytes(10));

    AtomicBoolean close = new AtomicBoolean(false);

    ResponseContentLifecycleManager<ReadObjectResponse> manager =
        resp -> ResponseContentLifecycleHandle.create(resp, () -> close.compareAndSet(false, true));

    try (GapicUnbufferedReadableByteChannel c =
        new GapicUnbufferedReadableByteChannel(
            SettableApiFuture.create(),
            new ZeroCopyServerStreamingCallable<>(
                new ServerStreamingCallable<ReadObjectRequest, ReadObjectResponse>() {
                  @Override
                  public void call(
                      ReadObjectRequest request,
                      ResponseObserver<ReadObjectResponse> respond,
                      ApiCallContext context) {
                    respond.onStart(new StreamController() {
                      @Override
                      public void cancel() {}

                      @Override
                      public void request(int count) {}

                      @Override
                      public void disableAutoInboundFlowControl() {}
                    });
                    respond.onResponse(
                        ReadObjectResponse.newBuilder()
                            .setChecksummedData(testContent.asChecksummedData())
                            .build());
                    respond.onComplete();
                  }
                },
                manager),
            ReadObjectRequest.getDefaultInstance(),
            Hasher.noop(),
            Retrier.attemptOnce(),
            Retrying.neverRetry())) {

      ByteBuffer buffer = ByteBuffer.allocate(15);
      c.read(buffer);
      assertThat(xxd(buffer)).isEqualTo(xxd(testContent.getBytes()));
      assertThat(close.get()).isTrue();
    }
  }

  @Test
  public void read_simulatesPacketDrop_prematureEOF() throws Exception {
    // 1. Setup
    final int totalObjectSize = 100;
    final int partSize = 10;
    final int numParts = 10;

    ServerStreamingCallable<ReadObjectRequest, ReadObjectResponse> mockCallable = mock(ServerStreamingCallable.class);

    ResponseContentLifecycleManager<ReadObjectResponse> manager = resp -> ResponseContentLifecycleHandle.create(resp, () -> {});

    ResultRetryAlgorithm<Object> resultRetryAlgorithm =
        new BasicResultRetryAlgorithm<Object>() {
          @Override
          public boolean shouldRetry(Throwable previousThrowable, Object previousResponse) {
            return previousThrowable instanceof SimulatedTimeoutException;
          }
        };

    SettableApiFuture<com.google.storage.v2.Object> result = SettableApiFuture.create();
    ReadObjectRequest req = ReadObjectRequest.newBuilder().setReadLimit(totalObjectSize).build();

    // 2. Mocking the Stream Behavior
    doAnswer(
        new Answer<Void>() {
          private int invocationCount = 0;

          @Override
          public Void answer(InvocationOnMock invocation) {
            invocationCount++;
            ResponseObserver<ReadObjectResponse> observer = invocation.getArgument(1);
            observer.onStart(new StreamController() {
              @Override
              public void cancel() {}

              @Override
              public void request(int count) {}

              @Override
              public void disableAutoInboundFlowControl() {}
            });

            if (invocationCount == 1) {
              for (int i = 0; i < numParts - 2; i++) {
                ReadObjectResponse response = ReadObjectResponse.newBuilder()
                    .setChecksummedData(
                        ChecksummedData.newBuilder()
                            .setContent(ByteString.copyFrom(new byte[partSize]))
                            .build())
                    .build();
                observer.onResponse(response);
              }
              observer.onError(new SimulatedTimeoutException("simulated timeout"));
            } else {
              observer.onComplete();
            }
            return null;
          }
        })
        .when(mockCallable)
        .call(any(ReadObjectRequest.class), any(ResponseObserver.class), any(ApiCallContext.class));

    // 3. Execution
    try (GapicUnbufferedReadableByteChannel channel =
        new GapicUnbufferedReadableByteChannel(
            result,
            new ZeroCopyServerStreamingCallable<>(mockCallable, manager),
            req,
            Hasher.noop(),
            Retrier.attemptOnce(),
            resultRetryAlgorithm)) {

      ByteBuffer buffer = ByteBuffer.allocate(totalObjectSize);
      int bytesRead = 0;
      while (buffer.hasRemaining()) {
        int readCount = channel.read(buffer);
        System.out.println("Reading from channel...");
        if (readCount == -1) {
          break;
        }
        bytesRead += readCount;
      }

      // 4. Assertion
      assertEquals(partSize * 8, bytesRead);
    }
  }
}
