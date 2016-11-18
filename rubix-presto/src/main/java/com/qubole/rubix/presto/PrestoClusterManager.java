/**
 * Copyright (c) 2016. Qubole Inc
 * Licensed under the Apache License, Version 2.0 (the License);
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an AS IS BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License. See accompanying LICENSE file.
 */
package com.qubole.rubix.presto;

import com.google.common.base.Throwables;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import com.qubole.rubix.spi.ClusterManager;
import com.qubole.rubix.spi.ClusterType;
import org.apache.commons.httpclient.HttpStatus;
import org.apache.commons.httpclient.params.HttpConnectionParams;
import org.apache.commons.httpclient.params.HttpParams;
import org.apache.commons.httpclient.params.HttpParamsFactory;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import java.lang.reflect.Type;
import java.net.InetAddress;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Created by stagra on 14/1/16.
 */
public class PrestoClusterManager extends ClusterManager
{
    private boolean isMaster = true;
    private int serverPort = 8081;
    private String serverAddress = "localhost";

    //private Supplier<List<String>> nodesSupplier;
    static LoadingCache<String, List<String>> nodesCache;

    private Log log = LogFactory.getLog(PrestoClusterManager.class);

    public static String serverPortConf = "caching.fs.presto-server-port";
    public static String serverAddressConf = "master.hostname";

    // Safe to use single instance of HttpClient since Supplier.get() provides synchronization
    @Override
    public void initialize(Configuration conf)
    {
        super.initialize(conf);
        this.serverPort = conf.getInt(serverPortConf, serverPort);
        this.serverAddress = conf.get(serverAddressConf, serverAddress);
        /*nodesSupplier = Suppliers.memoizeWithExpiration(new Supplier<List<String>>() {
            @Override
            public List<String> get()
            {*/
        ExecutorService executor = Executors.newSingleThreadExecutor();
        nodesCache = CacheBuilder.newBuilder()
                .refreshAfterWrite(getNodeRefreshTime(), TimeUnit.SECONDS)
                .build(CacheLoader.asyncReloading(new CacheLoader<String, List<String>>()
                {
                    @Override
                    public List<String> load(String s)
                            throws Exception
                    {
                        if (!isMaster) {
                    // First time all nodes start assuming themselves as master and down the line figure out their role
                    // Next time onwards, only master will be fetching the list of nodes
                    return ImmutableList.of();
                }

                try {
                    PoolingHttpClientConnectionManager connManager = new PoolingHttpClientConnectionManager(1, TimeUnit.MINUTES);
                    HttpGet allNodesRequest = new HttpGet(getNodeUri());
                    HttpGet failedNodesRequest = new HttpGet(getFailedNodeUri());
                    CloseableHttpClient httpClient = null;
                    log.info("initalize 1");
                        log.info("pool");
                        connManager.setMaxTotal(200);
                        connManager.setDefaultMaxPerRoute(100);
                        HttpParamsFactory factory = new HttpParamsFactory() {
                            @Override
                            public HttpParams getDefaultParams()
                            {
                                return null;
                            }
                        };
                        HttpConnectionParams.setHttpParamsFactory(factory);
                        log.info("123");
                        //httpClient = HttpClients.createSystem();
                        httpClient = HttpClients.custom().setConnectionManager(connManager).build();
                        //HttpClients.custom().setConnectionTimeToLive(1, TimeUnit.MINUTES).build();
                    log.info("done and here");
                    CloseableHttpResponse allNodesResponse = httpClient.execute(allNodesRequest);
                    CloseableHttpResponse failedNodesResponse = httpClient.execute(failedNodesRequest);

                    StringBuffer allResponse = new StringBuffer();
                    StringBuffer failedResponse = new StringBuffer();

                   if (allNodesResponse.getStatusLine().getStatusCode() == HttpStatus.SC_OK) {
                        isMaster = true;
                        BufferedReader in = new BufferedReader(new InputStreamReader(allNodesResponse.getEntity().getContent()));
                        String inputLine = "";
                        while ((inputLine = in.readLine()) != null) {
                            allResponse.append(inputLine);
                        }
                        in.close();
                       if (allNodesResponse.getEntity() != null) {
                           try {
                               allNodesResponse.getEntity().consumeContent();
                           }
                           catch (IOException e) {
                               log.info("abc");
                           }
                       }
                       allNodesResponse.close();

                    }
                    else {
                        log.info(String.format("v1/node failed with code: %d setting this node as worker ", allNodesResponse.getStatusLine().getStatusCode()));
                        isMaster = false;
                       allNodesResponse.close();
                        return ImmutableList.of();
                    }
                    log.info("I can be here too");

                    // check on failed nodes
                    if (failedNodesResponse.getStatusLine().getStatusCode() == HttpStatus.SC_OK) {
                        BufferedReader in = new BufferedReader(new InputStreamReader(failedNodesResponse.getEntity().getContent()));
                        String inputLine;
                        while ((inputLine = in.readLine()) != null) {
                            failedResponse.append(inputLine);
                        }
                        in.close();
                    }
                    failedNodesResponse.close();
                    /*URL allNodesRequest = getNodeUrl();
                    URL failedNodesRequest = getFailedNodeUrl();
                    HttpURLConnection httpcon = (HttpURLConnection) allNodesRequest.openConnection();
                    HttpURLConnection httpconf = (HttpURLConnection) failedNodesRequest.openConnection();
                    httpcon.setConnectTimeout(500);
                    httpconf.setConnectTimeout(500);
                    httpcon.setRequestMethod("GET");
                    httpconf.setRequestMethod("GET");
                    int allNodesResponseCode = httpcon.getResponseCode();
                    int failedNodesResponseCode = httpconf.getResponseCode();

                    StringBuffer allResponse = new StringBuffer();
                    StringBuffer failedResponse = new StringBuffer();

                    if (allNodesResponseCode == HttpURLConnection.HTTP_OK) {
                        isMaster = true;
                        BufferedReader in = new BufferedReader(new InputStreamReader(httpcon.getInputStream()));
                        String inputLine = "";
                        while ((inputLine = in.readLine()) != null) {
                            allResponse.append(inputLine);
                        }
                        in.close();
                        httpcon.disconnect();

                    }
                    else {
                        log.info(String.format("v1/node failed with code: setting this node as worker "));
                        isMaster = false;
                        //allNodesResponse.close();
                        httpcon.disconnect();
                        return ImmutableList.of();
                    }

                    // check on failed nodes
                    if (failedNodesResponseCode == HttpURLConnection.HTTP_OK) {
                        BufferedReader in = new BufferedReader(new InputStreamReader(httpconf.getInputStream()));
                        String inputLine;
                        while ((inputLine = in.readLine()) != null) {
                            failedResponse.append(inputLine);
                        }
                        in.close();
                    }
                    httpconf.disconnect();*/
                    Gson gson = new Gson();
                    Type type = new TypeToken<List<Stats>>() {}.getType();

                    List<Stats> allNodes = gson.fromJson(allResponse.toString(), type);
                    List<Stats> failedNodes = gson.fromJson(failedResponse.toString(), type);
                    if (allNodes.isEmpty()) {
                        // Empty result set => server up and only master node running, return localhost has the only node
                        // Do not need to consider failed nodes list as 1node cluster and server is up since it replied to allNodesRequest
                        return ImmutableList.of(InetAddress.getLocalHost().getHostName());
                    }

                    if (failedNodes.isEmpty()) {
                        failedNodes = ImmutableList.of();
                        //return ImmutableList.of(InetAddress.getLocalHost().getHostName());
                    }

                    // keep only the healthy nodes
                    allNodes.removeAll(failedNodes);

                    Set<String> hosts = new HashSet<String>();

                    for (Stats node : allNodes) {
                        hosts.add(node.getUri().getHost());
                        log.info(String.format("Node: %s", node.getUri()));
                    }
                    if (hosts.isEmpty()) {
                        // case of master only cluster
                        hosts.add(InetAddress.getLocalHost().getHostName());
                    }
                    List<String> hostList = Lists.newArrayList(hosts.toArray(new String[0]));
                    Collections.sort(hostList);
                    return hostList;
                }
                catch (IOException e) {
                    throw Throwables.propagate(e);
                }
                    }
                }, executor));
    //        }
      //  }, 10, TimeUnit.SECONDS);
    }

    @Override
    public boolean isMaster()
    {
        // issue get on nodesSupplier to ensure that isMaster is set correctly
        //nodesSupplier.get();
        try {
            nodesCache.get("nodeList");
        }
        catch (ExecutionException e) {
            e.printStackTrace();
        }
        return isMaster;
    }

    /*
     * This returns list of worker nodes when there are worker nodes in the cluster
     * If it is a single node cluster, it will return localhost information
     */
    @Override
    public List<String> getNodes()
    {
        //return nodesSupplier.get();
        try {
            return nodesCache.get("nodeList");
        }
        catch (ExecutionException e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public ClusterType getClusterType()
    {
        return ClusterType.PRESTO_CLUSTER_MANAGER;
    }

    private URI getNodeUri()
            throws URISyntaxException
    {
        return new URI("http://" + serverAddress + ":" + serverPort + "/v1/node");
    }

    private URI getFailedNodeUri()
            throws URISyntaxException
    {
        return new URI("http://" + serverAddress + ":" + serverPort + "/v1/node/failed");
    }

    private URL getNodeUrl()
            throws MalformedURLException
    {
        return new URL("http://" + serverAddress + ":" + serverPort + "/v1/node");
    }

    private URL getFailedNodeUrl()
            throws MalformedURLException
    {
        return new URL("http://" + serverAddress + ":" + serverPort + "/v1/node/failed");
    }

    public static class Stats
    {
        URI uri;
        String lastResponseTime;

        public Stats()
        {
        }

        public Stats(URI uri, String lastResponseTime)
        {
            this.uri = uri;
            this.lastResponseTime = lastResponseTime;
        }

        public URI getUri()
        {
            return uri;
        }

        public void setURI(URI uri)
        {
            this.uri = uri;
        }

        String getLastResponseTime()
        {
            return lastResponseTime;
        }

        public void setLastResponseTime(String lastResponseTime)
        {
            this.lastResponseTime = lastResponseTime;
        }

        @Override
        public boolean equals(Object other)
        {
            if (this == other) {
                return true;
            }
            if (other == null || getClass() != other.getClass()) {
                return false;
            }
            Stats o = (Stats) other;
            return uri.equals(o.getUri()) && lastResponseTime.equals(o.getLastResponseTime());
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(uri, lastResponseTime);
        }
    }
}
