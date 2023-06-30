package com.example.kafkademo;

import org.apache.kafka.clients.admin.*;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.util.*;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.DescribeConsumerGroupsOptions;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartitionInfo;

import java.util.Collections;
import java.util.Properties;

@SpringBootApplication
public class KafkaDemoApplication {

	public static void main(String[] args) throws ExecutionException, InterruptedException {
		Properties config = new Properties();
		config.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		AdminClient adminClient = KafkaAdminClient.create(config);

		String topic = "kafkademotopic";
		int partition = 0;

		getGroupCoordinator(adminClient);
		getTopicLeader(adminClient, topic, partition);

		// Run spring boot application
		SpringApplication.run(KafkaDemoApplication.class, args);
	}

	private static void getGroupCoordinator(AdminClient adminClient) throws ExecutionException, InterruptedException {
		DescribeConsumerGroupsOptions options = new DescribeConsumerGroupsOptions();

		// Find group coordinator
		KafkaFuture<Map<String, ConsumerGroupDescription>> future = adminClient.describeConsumerGroups(Collections.singletonList("myGroup"), options)
				.all();

		System.out.println("--------------------------------------------");
		for (Map.Entry<String,ConsumerGroupDescription> entry : future.get().entrySet()) {
			System.out.println("Group Coordinator: " + entry.getValue().coordinator());
		}
		System.out.println("--------------------------------------------");
	}

	private static void getTopicLeader(AdminClient adminClient, String topic, int partition) throws ExecutionException, InterruptedException {
		DescribeTopicsOptions describeTopicsOptions = new DescribeTopicsOptions().timeoutMs(5000);
		DescribeTopicsResult describeTopicsResult = adminClient.describeTopics(Collections.singleton(topic), describeTopicsOptions);
		KafkaFuture<TopicDescription> topicDescriptionFuture = describeTopicsResult.values().get(topic);

		TopicDescription topicDescription = topicDescriptionFuture.get();
		TopicPartitionInfo partitionInfo = topicDescription.partitions().get(partition);

		Node leader = partitionInfo.leader();
		System.out.println("--------------------------------------------");
		System.out.println("Leader for Topic " + topic + ", Partition " + partition + ": " + leader.id() + " - " + leader.host());
		System.out.println("--------------------------------------------");

	}

}
