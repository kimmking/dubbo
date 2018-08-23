/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.dubbo.rpc.cluster.router.tag;


import org.apache.dubbo.common.Constants;
import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.logger.Logger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.common.utils.StringUtils;
import org.apache.dubbo.rpc.Invocation;
import org.apache.dubbo.rpc.Invoker;
import org.apache.dubbo.rpc.RpcContext;
import org.apache.dubbo.rpc.RpcException;
import org.apache.dubbo.rpc.cluster.Router;

import java.util.ArrayList;
import java.util.List;


public class TagRouter implements Router, Comparable<Router> {

    private static final Logger logger = LoggerFactory.getLogger(TagRouter.class);

    public static final String NAME = "tag";

    private final int priority;
    private final URL url;

    public static final URL ROUTER_URL = new URL(NAME, Constants.ANYHOST_VALUE, 0, Constants.ANY_VALUE).addParameters(Constants.RUNTIME_KEY, "true");

    public TagRouter(URL url) {
        this.url = url;
        this.priority = url.getParameter(Constants.PRIORITY_KEY, 0);
    }

    public TagRouter() {
        this(ROUTER_URL);
    }

    @Override
    public URL getUrl() {
        return url;
    }

    @Override
    public <T> List<Invoker<T>> route(List<Invoker<T>> invokers, URL url, Invocation invocation) throws RpcException {

        List<Invoker<T>> result = new ArrayList<>();
        try {
            String tag = RpcContext.getContext().getAttachment(Constants.REQUEST_TAG_KEY);
            String forceTagStr = RpcContext.getContext().getAttachment(Constants.TAG_FORCE_KEY);
            boolean forceTag = StringUtils.isEmpty(forceTagStr)?false:Boolean.parseBoolean(forceTagStr);

            if (StringUtils.isEmpty(tag)) {
                if(forceTag) {
                    for (Invoker<T> invoker : invokers) {
                        if (StringUtils.isEmpty(invoker.getUrl().getParameter(Constants.TAG_KEY))) {
                            result.add(invoker);
                        }
                    }
                }else{
                    result = invokers;
                }
            } else {
                for (Invoker<T> invoker : invokers) {
                    if (tag.equals(invoker.getUrl().getParameter(Constants.TAG_KEY))) {
                        result.add(invoker);
                    }
                }
                if(result.isEmpty()&&!forceTag) result = invokers;
            }

            return result;
        } catch (Exception e) {
            logger.error("Route by tag error,return all invokers.", e);
        }
        // Downgrade to all invokers
        return invokers;
    }

    @Override
    public int compareTo(Router o) {
        if (o == null || o.getClass() != TagRouter.class) {
            return 1;
        }
        TagRouter c = (TagRouter) o;
        return this.priority == c.priority ? url.toFullString().compareTo(c.url.toFullString()) : (this.priority > c.priority ? 1 : -1);
    }
}
