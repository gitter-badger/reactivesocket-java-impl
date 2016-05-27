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
package io.reactivesocket.netty.udt.client;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.udt.UdtMessage;
import io.reactivesocket.Frame;
import io.reactivesocket.netty.MutableDirectByteBuf;
import io.reactivesocket.rx.Observer;

import java.util.concurrent.CopyOnWriteArrayList;

@ChannelHandler.Sharable
public class ReactiveSocketClientHandler extends SimpleChannelInboundHandler<UdtMessage> {

    private final CopyOnWriteArrayList<Observer<Frame>> subjects;

    public ReactiveSocketClientHandler(CopyOnWriteArrayList<Observer<Frame>> subjects) {
        this.subjects = subjects;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, UdtMessage msg) throws Exception {
        ByteBuf content = msg.content();
        MutableDirectByteBuf mutableDirectByteBuf = new MutableDirectByteBuf(content);
        final Frame from = Frame.from(mutableDirectByteBuf, 0, mutableDirectByteBuf.capacity());
        subjects.forEach(o -> o.onNext(from));
    }
}
