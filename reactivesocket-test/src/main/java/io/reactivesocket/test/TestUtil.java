/**
 * Copyright 2015 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.reactivesocket.test;

import io.reactivesocket.Frame;
import io.reactivesocket.FrameType;
import io.reactivesocket.Payload;
import org.agrona.MutableDirectBuffer;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

public class TestUtil
{
    public static Frame utf8EncodedRequestFrame(final int streamId, final FrameType type, final String data, final int initialRequestN)
    {
        return Frame.Request.from(streamId, type, new Payload()
        {
            public ByteBuffer getData()
            {
                return byteBufferFromUtf8String(data);
            }

            public ByteBuffer getMetadata()
            {
                return Frame.NULL_BYTEBUFFER;
            }
        }, initialRequestN);
    }

    public static Frame utf8EncodedResponseFrame(final int streamId, final FrameType type, final String data)
    {
        return Frame.Response.from(streamId, type, utf8EncodedPayload(data, null));
    }

    public static Frame utf8EncodedErrorFrame(final int streamId, final String data)
    {
        return Frame.Error.from(streamId, new Exception(data));
    }

    public static Payload utf8EncodedPayload(final String data, final String metadata)
    {
        return new PayloadImpl(data, metadata);
    }

    public static String byteToString(ByteBuffer byteBuffer)
    {
        byteBuffer = byteBuffer.duplicate();

        final byte[] bytes = new byte[byteBuffer.remaining()];
        byteBuffer.get(bytes);
        return new String(bytes, StandardCharsets.UTF_8);
    }

    public static ByteBuffer byteBufferFromUtf8String(final String data)
    {
        final byte[] bytes = data.getBytes(StandardCharsets.UTF_8);
        return ByteBuffer.wrap(bytes);
    }

    public static void copyFrame(final MutableDirectBuffer dst, final int offset, final Frame frame)
    {
        dst.putBytes(offset, frame.getByteBuffer(), frame.offset(), frame.length());
    }

    private static class PayloadImpl implements Payload // some JDK shoutout
    {
        private ByteBuffer data;
        private ByteBuffer metadata;

        public PayloadImpl(final String data, final String metadata)
        {
            if (null == data)
            {
                this.data = ByteBuffer.allocate(0);
            }
            else
            {
                this.data = byteBufferFromUtf8String(data);
            }

            if (null == metadata)
            {
                this.metadata = ByteBuffer.allocate(0);
            }
            else
            {
                this.metadata = byteBufferFromUtf8String(metadata);
            }
        }

        public boolean equals(Object obj)
        {
            System.out.println("equals: " + obj);
            final Payload rhs = (Payload)obj;

            return (TestUtil.byteToString(data).equals(TestUtil.byteToString(rhs.getData()))) &&
                (TestUtil.byteToString(metadata).equals(TestUtil.byteToString(rhs.getMetadata())));
        }

        public ByteBuffer getData()
        {
            return data;
        }

        public ByteBuffer getMetadata()
        {
            return metadata;
        }
    }

    public static <T> T toSingleBlocking(Publisher<T> source) {
        return toListBlocking(source).get(0);
    }

    public static <T> List<T> toListBlocking(Publisher<T> source) {
        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<List<T>> ref = new AtomicReference<>();
        AtomicReference<Throwable> throwable = new AtomicReference<>();

        source.subscribe(new Subscriber<T>() {
            private List<T> buffer = new ArrayList<T>();

            @Override
            public void onSubscribe(Subscription s) {
                s.request(Long.MAX_VALUE);
            }

            @Override
            public void onNext(T t) {
                buffer.add(t);
            }

            @Override
            public void onError(Throwable t) {
                throwable.set(t);
                latch.countDown();
            }

            @Override
            public void onComplete() {
                ref.set(buffer);
                latch.countDown();
            }
        });

        try {
            latch.await();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        List<T> values = ref.get();
        Throwable t = throwable.get();

        if (values == null) {
            RuntimeException r;
            if (t != null) {
                r = new RuntimeException(t);
            } else {
                r = new RuntimeException();
            }
            throw r;
        } else {
            return ref.get();
        }
    }
}
