package ru.bltpzdc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Driver {

    private static final Logger LOG = LoggerFactory.getLogger(Driver.class);

    public static void main(String[] args) throws Exception {

        if (args.length < 3) {
            System.err.println("Usage: hadoop jar <path-to-jar> <input> <output> <metric>");
            System.exit(1);
        }

        String input = args[0];
        String output = args[1];
        String metric = args[2];

        LOG.info("Job started");
        LOG.info("Input: {}", input);
        LOG.info("Output: {}", output);
        LOG.info("Metric: {}", metric);

        Configuration conf = new Configuration();
        conf.set("metric", metric);

        Job job = Job.getInstance(conf, "Transactions processing");
        job.setJarByClass(Driver.class);

        job.setMapperClass(CategoryMapper.class);
        job.setReducerClass(CategoryReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DoubleWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        FileInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));

        boolean success = job.waitForCompletion(true);
        LOG.info("Job completed: {}", success);
        System.exit(success ? 0 : 1);
    }
}
