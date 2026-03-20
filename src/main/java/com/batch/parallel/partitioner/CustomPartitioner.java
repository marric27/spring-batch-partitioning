package com.batch.parallel.partitioner;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

import org.springframework.batch.core.partition.support.Partitioner;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.core.io.Resource;

public class CustomPartitioner implements Partitioner {

    private static final String DEFAULT_FILE_NAME = "people-10000.csv";

    private final Resource resource;

    public CustomPartitioner(Resource resource) {
        this.resource = resource;
    }

    @Override
    public Map<String, ExecutionContext> partition(int gridSize) {
        Map<String, ExecutionContext> partitions = new HashMap<>();
        int totalItems = countItems();

        if (totalItems == 0) {
            ExecutionContext context = new ExecutionContext();
            context.putString("fileName", DEFAULT_FILE_NAME);
            context.putInt("partitionNumber", 0);
            context.putInt("startItem", 0);
            context.putInt("endItem", 0);
            partitions.put("partition0", context);
            return partitions;
        }

        int targetSize = Math.max(1, (int) Math.ceil((double) totalItems / gridSize));
        int startItem = 0;
        int partitionNumber = 0;

        while (startItem < totalItems) {
            int endItem = Math.min(startItem + targetSize, totalItems);
            ExecutionContext context = new ExecutionContext();
            context.putString("fileName", DEFAULT_FILE_NAME);
            context.putInt("partitionNumber", partitionNumber);
            context.putInt("startItem", startItem);
            context.putInt("endItem", endItem);
            partitions.put("partition" + partitionNumber, context);
            startItem = endItem;
            partitionNumber++;
        }

        return partitions;
    }

    private int countItems() {
        try (BufferedReader reader = new BufferedReader(
                new InputStreamReader(resource.getInputStream(), StandardCharsets.UTF_8))) {
            return Math.toIntExact(Math.max(0, reader.lines().skip(1).count()));
        } catch (IOException exception) {
            throw new UncheckedIOException("Unable to count items for partitioning", exception);
        }
    }
}
