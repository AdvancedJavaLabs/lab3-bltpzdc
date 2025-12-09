package ru.bltpzdc;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CategoryReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

    private static final Logger LOG = LoggerFactory.getLogger(Reducer.class);

    private String metric;

    @Override
    protected void setup(Context context) {
        metric = context.getConfiguration().get("metric", "sum");
        LOG.info("Used action: {}", metric);
    }

    @Override
    protected void reduce(Text category,
                          Iterable<DoubleWritable> prices,
                          Context context)
            throws IOException, InterruptedException {

        double sum = 0;
        int count = 0;

        for (DoubleWritable p : prices) {
            sum += p.get();
            ++count;
        }

        double result;

        switch (metric) {
            case "avg":
                result = sum / count;
                break;
            case "count":
                result = count;
                break;
            default:
                result = sum;
        }

        context.write(category, new DoubleWritable(result));
    }
}
