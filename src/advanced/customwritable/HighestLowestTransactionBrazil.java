package advanced.customwritable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.log4j.BasicConfigurator;


import java.io.IOException;

public class HighestLowestTransactionBrazil {
    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure();
        Configuration conf = new Configuration();

        String[] files = new GenericOptionsParser(conf, args).getRemainingArgs();

        Job job = Job.getInstance(conf, "HighestLowestTransactionBrazil");

        Path input = new Path(files[0]);
        Path output = new Path(files[1]);

        job.setJarByClass(HighestLowestTransactionBrazil.class);
        job.setMapperClass(MapperHighestLowestTransactionBrazil.class);
        job.setReducerClass(ReducerHighestLowestTransactionBrazil.class);


        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(TransactionWritable.class);

        FileInputFormat.addInputPath(job, input);
        FileOutputFormat.setOutputPath(job, output);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    public static class MapperHighestLowestTransactionBrazil extends Mapper<LongWritable, Text, Text,TransactionWritable> {
        private boolean isFirstLine = true;

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String linha = value.toString();
            String[] fields = linha.split(";");

            if (isFirstLine) {
                isFirstLine = false;
                return;
            }

            if (fields.length == 10 && fields[0].equals("Brazil") && fields[1].equals("2016") && !fields[5].isEmpty()) {
                double tradeValue = Double.parseDouble(fields[5]);
                context.write(new Text("Brazil_2016"), new TransactionWritable(2016, "Brazil", tradeValue));
            }
        }
    }

    public static class ReducerHighestLowestTransactionBrazil extends Reducer<Text, TransactionWritable, Text, Text> {

        protected void reduce(Text key, Iterable<TransactionWritable> values, Context context) throws IOException, InterruptedException {
            double maxTransaction = Double.MIN_VALUE;
            double minTransaction = Double.MAX_VALUE;

            for (TransactionWritable value : values) {
                double amount = value.getTradeValue();

                
                if (amount > maxTransaction) {
                    maxTransaction = amount;
                }

                if (amount < minTransaction) {
                    minTransaction = amount;
                }
            }

            context.write(new Text("Max Transaction in 2016 for Brazil"), new Text(Double.toString(maxTransaction)));
            context.write(new Text("Min Transaction in 2016 for Brazil"), new Text(Double.toString(minTransaction)));
        }
    }
}
