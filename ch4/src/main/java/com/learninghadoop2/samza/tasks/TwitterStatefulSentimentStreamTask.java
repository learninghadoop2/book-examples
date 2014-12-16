package com.learninghadoop2.samza.tasks;

//~--- non-JDK imports --------------------------------------------------------

import org.apache.samza.config.Config;
import org.apache.samza.storage.kv.KeyValueStore;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.SystemStream;
import org.apache.samza.task.InitableTask;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.StreamTask;
import org.apache.samza.task.TaskContext;
import org.apache.samza.task.TaskCoordinator;
import org.apache.samza.task.WindowableTask;

//~--- JDK imports ------------------------------------------------------------

import java.util.HashSet;
import java.util.Set;

public class TwitterStatefulSentimentStreamTask implements StreamTask, WindowableTask, InitableTask {
    private Set<String>                    positiveWords  = new HashSet<String>();
    private Set<String>                    negativeWords  = new HashSet<String>();
    private int                            tweets         = 0;
    private int                            positiveTweets = 0;
    private int                            negativeTweets = 0;
    private int                            maxPositive    = 0;
    private int                            maxNegative    = 0;
    private KeyValueStore<String, Integer> store;

    @SuppressWarnings("unchecked")
    @Override
    public void init(Config config, TaskContext context) {
        this.store = (KeyValueStore<String, Integer>) context.getStore("tweet-store");
    }

    @Override
    public void process(IncomingMessageEnvelope envelope, MessageCollector collector, TaskCoordinator coordinator) {
        if ("positive-words".equals(envelope.getSystemStreamPartition().getStream())) {
            positiveWords.add(((String) envelope.getMessage()));
        } else if ("negative-words".equals(envelope.getSystemStreamPartition().getStream())) {
            negativeWords.add(((String) envelope.getMessage()));
        } else if ("english-tweets".equals(envelope.getSystemStreamPartition().getStream())) {
            tweets++;

            int    positive = 0;
            int    negative = 0;
            String words    = ((String) envelope.getMessage());

            for (String word : words.split(" ")) {
                if (positiveWords.contains(word)) {
                    positive++;
                } else if (negativeWords.contains(word)) {
                    negative++;
                }
            }

            if (positive > negative) {
                positiveTweets++;
            }

            if (negative > positive) {
                negativeTweets++;
            }

            if (positive > maxPositive) {
                maxPositive = positive;
            }

            if (negative > maxNegative) {
                maxNegative = negative;
            }
        }
    }

    @Override
    public void window(MessageCollector collector, TaskCoordinator coordinator) {
        Integer lifetimeMaxPositive = store.get("lifetimeMaxPositive");
        Integer lifetimeMaxNegative = store.get("lifetimeMaxNegative");

        if ((lifetimeMaxPositive == null) || (maxPositive > lifetimeMaxPositive)) {
            lifetimeMaxPositive = maxPositive;
            store.put("lifetimeMaxPositive", lifetimeMaxPositive);
        }

        if ((lifetimeMaxNegative == null) || (maxNegative > lifetimeMaxNegative)) {
            lifetimeMaxNegative = maxNegative;
            store.put("lifetimeMaxNegative", lifetimeMaxNegative);
        }

        String msg =
            String.format(
                "Tweets: %d Positive: %d Negative: %d MaxPositive: %d MaxNegative: %d LifetimeMaxPositive: %d LifetimeMaxNegative: %d",
                tweets, positiveTweets, negativeTweets, maxPositive, maxNegative, lifetimeMaxPositive,
                lifetimeMaxNegative);

        collector.send(new OutgoingMessageEnvelope(new SystemStream("kafka", "tweet-stateful-sentiment-stats"), msg));

        // Reset counts after windowing.
        tweets         = 0;
        positiveTweets = 0;
        negativeTweets = 0;
        maxPositive    = 0;
        maxNegative    = 0;
    }
}


