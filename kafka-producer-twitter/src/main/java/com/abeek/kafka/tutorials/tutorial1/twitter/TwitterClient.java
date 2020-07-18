package com.abeek.kafka.tutorials.tutorial1.twitter;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;

import java.util.List;
import java.util.concurrent.BlockingQueue;

public class TwitterClient {

    private String consumerKey = "gQ8JGmW6yi5Orzhly0GAociiW";
    private String consumerSecret = "Zi3m0DGAdGHNLMmUGAIpZta41RQvxG2LB186Dzzz51sU05jSx7";
    private String token = "140518270-onKtbeqavXrGn7cMKw2pq9SvJmHq0Q6UpbLDDB6T";
    private String secret = "E5qFEXI2RLqX0XO6CPLAYqXLJeLnLGBGMXfviGNZrtPAX";

    public TwitterClient () {

    }

    /** Set up your blocking queues: Be sure to size these properly based on expected TPS of your stream */
    //private BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(100000);

    public Client createClient(BlockingQueue<String> msgQueue) {
        Authentication hosebirdAuth = new OAuth1(consumerKey, consumerSecret, token, secret);
        Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
        List<String> terms = Lists.newArrayList("trump");
        StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();
        hosebirdEndpoint.trackTerms(terms);
        ClientBuilder builder = new ClientBuilder()
                .hosts(hosebirdHosts)
                .authentication(hosebirdAuth)
                .endpoint(hosebirdEndpoint)
                .processor(new StringDelimitedProcessor(msgQueue));

        Client hosebirdClient = builder.build();
        return hosebirdClient;
    }
}
