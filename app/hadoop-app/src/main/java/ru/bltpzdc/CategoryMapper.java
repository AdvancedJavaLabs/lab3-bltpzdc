package ru.bltpzdc;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CategoryMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {

    private static final Logger LOG = LoggerFactory.getLogger(CategoryMapper.class);

    private final Text category = new Text();
    private final DoubleWritable price = new DoubleWritable();

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        String line = value.toString();

        if (line.startsWith("transaction_id")) return;

        try {
            String[] parts = line.split(",");
            if (parts.length < 5) {
                LOG.warn("Invalid line: {}", line);
                return;
            }

            String categ = parts[2];
            double priceValue = Double.parseDouble(parts[3]);
            double count = Integer.parseInt(parts[4]);

            category.set(categ);
            price.set(priceValue * count);

            context.write(category, price);
        } catch (IOException | InterruptedException | NumberFormatException e) {
            LOG.error("Failed parse line: {}", line, e);
        }
    }
}
