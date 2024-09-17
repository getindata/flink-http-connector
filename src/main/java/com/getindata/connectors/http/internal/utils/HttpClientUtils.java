/*
 * ----------------------------------------------------------------------------
 *   ____________ ______ _  _______             _____  _  __   ____   _____
 *  |___  /  ____|  ____| |/ /  __ \      /\   |  __ \| |/ /  / __ \ / ____|
 *     / /| |__  | |__  | ' /| |__) |    /  \  | |__) | ' /  | |  | | (___
 *    / / |  __| |  __| |  < |  _  /    / /\ \ |  _  /|  <   | |  | |\___ \
 *   / /__| |____| |____| . \| | \ \   / ____ \| | \ \| . \  | |__| |____) |
 *  /_____|______|______|_|\_\_|  \_\ /_/    \_\_|  \_\_|\_\  \____/|_____/
 *
 * Copyright (c) 2022-present, ZEEKR Inc. All rights reserved.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 * ----------------------------------------------------------------------------
 */

package com.getindata.connectors.http.internal.utils;

import okhttp3.Callback;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

/**
 * @author xueliang
 * @since 2024/9/16 14:38
 */
public class HttpClientUtils {

    public static CompletableFuture<Response> sendAsyncRequest(OkHttpClient client, Request request) {
        CompletableFuture<Response> future = new CompletableFuture<>();
        client.newCall(request).enqueue(new Callback() {
            @Override
            public void onFailure(okhttp3.Call call, IOException e) {
                future.completeExceptionally(e);
            }

            @Override
            public void onResponse(okhttp3.Call call, Response response) throws IOException {
                future.complete(response);
            }
        });
        return future;
    }

    public static List<CompletableFuture<Response>> batchSendAsyncRequest(OkHttpClient client, List<Request> requestList) {
        return requestList.stream().map(item -> sendAsyncRequest(client, item)).collect(Collectors.toList());
    }
}
