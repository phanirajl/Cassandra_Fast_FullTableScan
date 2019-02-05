package com.cassandra.utility.trial;

import com.cassandra.utility.policy.LocalOnlyPolicy;
import com.datastax.driver.core.*;

import java.io.*;
import java.util.*;


public class Trials {
//    private final Map<String,String> properties;
    private final Map<String,/*TreeSet*/HashSet<MyTokenRange>> hostToTokenRange;
    private final Map<Host,List<Cluster>> hostToClusters;
//    private final String contactPoint;
//    private final String username;
//    private final String password;
    Cluster tempClusterObj;
    Trials(String contactPoint,String username,String password, String nodetoolDescriberingFile,int consumersPerNode) throws IOException {
//        this.contactPoint = contactPoint;
//        this.username = username;
//        this.password = password;
        hostToTokenRange  = new HashMap<>();
        parseNodetoolDescriberingFile(nodetoolDescriberingFile);
//        new LocalOnlyPolicy(contactPoint,username,password);
        hostToClusters = initHostToClusters(contactPoint,username,password,consumersPerNode);

    }



    private Map<Host, List<Cluster>> initHostToClusters(String contactPoint, String username, String password, int consumersPerNode) {
        LocalOnlyPolicy localOnlyPolicy = new LocalOnlyPolicy(contactPoint,username,password);
        final Map<Host,List<Cluster>> hostToClusters = new HashMap<>();
        for(Host host : LocalOnlyPolicy.getHostsByClusterName(LocalOnlyPolicy.getClusterNameByHostIp(contactPoint))){
            List<Cluster> clustersPerHost = new ArrayList<>();
            for(int i=1;i<=consumersPerNode;i++) {
                Cluster individualHostCluster = Cluster.builder()
                        .addContactPoint(contactPoint) //OR host.toString().substring(1).split(":")[0]
                        .withQueryOptions(new QueryOptions().setFetchSize(5000))
                        .withCredentials(username, password)
                        .withLoadBalancingPolicy(new LocalOnlyPolicy(host.toString().substring(1).split(":")[0], username, password))
                        .withSocketOptions(new SocketOptions().setReadTimeoutMillis(1000 * 60 * 60).setConnectTimeoutMillis(1000 * 60 * 60))
                        .build();
                clustersPerHost.add(individualHostCluster);

            }
            hostToClusters.put(host, clustersPerHost);
        }
        return hostToClusters;
    }

    public Map<Host,List<Cluster>> getHostToClusters(){
        return hostToClusters;
    }

    public Map<String, /*Set*/HashSet<MyTokenRange>> getHostToTokenRange() {
        return hostToTokenRange;
    }

    private void parseNodetoolDescriberingFile(String nodetoolDescriberingFile) throws IOException {
        BufferedReader fileIn = new BufferedReader(new FileReader(nodetoolDescriberingFile));
        String line;
        while((line = fileIn.readLine())!=null){

//            	line = "TokenRange(start_token:1943978523300203561, end_token:2137919499801737315, endpoints:[127.0.0.3, 127.0.0.6, 127.0.0.7, 127.0.0.2, 127.0.0.5, 127.0.0.1], rpc_endpoints:[127.0.0.3, 127.0.0.6, 127.0.0.7, 127.0.0.2, 127.0.0.5, 127.0.0.1], endpoint_details:[EndpointDetails(host:127.0.0.3, datacenter:dc1, rack:r1), EndpointDetails(host:127.0.0.6, datacenter:dc2, rack:r1), EndpointDetails(host:127.0.0.7, datacenter:dc2, rack:r1), EndpointDetails(host:127.0.0.2, datacenter:dc1, rack:r1), EndpointDetails(host:127.0.0.5, datacenter:dc2, rack:r1), EndpointDetails(host:127.0.0.1, datacenter:dc1, rack:r1)])";

            //no sanity done
            MyTokenRange myTokenRange = new MyTokenRange(Long.parseLong(line.split("start_token:")[1].split(",")[0]),Long.parseLong(line.split("end_token:")[1].split(",")[0]));
            String hostIP = line.split("endpoints:")[1].split(",")[0].substring(1);
            hostToTokenRange.putIfAbsent(hostIP,new /*TreeSet*/HashSet<>());
            hostToTokenRange.get(hostIP).add(myTokenRange);
        }
    }


}
