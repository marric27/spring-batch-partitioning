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
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;

public class CustomPartitioner implements Partitioner {

    private static final Resource INPUT_RESOURCE = new ClassPathResource("people-10000.csv");

    @Override
    public Map<String, ExecutionContext> partition(int gridSize) {
        Map<String, ExecutionContext> partitions = new HashMap<>();
        int totalLines = countLines();

        if (totalLines == 0) {
            ExecutionContext context = new ExecutionContext();
            context.putInt("startLine", 1);
            context.putInt("endLine", 0);
            partitions.put("partition0", context);
            return partitions;
        }

        int targetSize = (int) Math.ceil((double) totalLines / gridSize);
        int partitionNumber = 0;

        for (int startLine = 1; startLine <= totalLines; startLine += targetSize) {
            int endLine = Math.min(startLine + targetSize - 1, totalLines);
            ExecutionContext context = new ExecutionContext();
            context.putInt("partitionNumber", partitionNumber);
            context.putInt("startLine", startLine);
            context.putInt("endLine", endLine);
            partitions.put("partition" + partitionNumber, context);
            partitionNumber++;
        }

        return partitions;
    }

    private int countLines() {
        try (BufferedReader reader = new BufferedReader(
                new InputStreamReader(INPUT_RESOURCE.getInputStream(), StandardCharsets.UTF_8))) {
            return (int) reader.lines().count();
        } catch (IOException exception) {
            throw new UncheckedIOException("Unable to count lines for partitioning", exception);
        }
    }
}
