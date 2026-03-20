package com.batch.parallel.partitioner;

import java.util.HashMap;
import java.util.Map;

import org.springframework.batch.core.partition.support.Partitioner;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.Resource;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class CustomPartitioner implements Partitioner {

    @Value("${batch.input.file.lines}")
    private int lines;

    private final Resource resource;

    public CustomPartitioner(Resource resource) {
        this.resource = resource;
    }

    @Override
    public Map<String, ExecutionContext> partition(int gridSize) {
        Map<String, ExecutionContext> partitions = new HashMap<>();
        int totalItems = lines;

        if (totalItems == 0) {
            ExecutionContext context = new ExecutionContext();
            context.putString("fileName", resource.getFilename());
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
            context.putString("fileName", resource.getFilename());
            context.putInt("partitionNumber", partitionNumber);
            context.putInt("startItem", startItem);
            context.putInt("endItem", endItem);


            log.info(String.format(">>> [PARTITIONER] Creata Partition %d: startItem=%d, endItem=%d (Size=%d)", 
                partitionNumber, startItem, endItem, (endItem - startItem)));

            partitions.put("partition" + partitionNumber, context);
            startItem = endItem;
            partitionNumber++;
        }

        return partitions;
    }
}
