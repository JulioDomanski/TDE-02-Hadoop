package advanced.customwritable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
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

public class HighLowTransactionAmountCountry {

    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure();
        Configuration conf = new Configuration();

        String[] files = new GenericOptionsParser(conf, args).getRemainingArgs();

        Job job = Job.getInstance(conf, "MinMaxPricePerYearAndCountry");

        Path input = new Path(files[0]);
        Path output = new Path(files[1]);

        job.setJarByClass(HighLowTransactionAmountCountry.class);
        job.setMapperClass(MapperMinMaxPrice.class);
        job.setReducerClass(ReducerMinMaxPrice.class);

        job.setMapOutputKeyClass(HLAWritable.class);
        job.setMapOutputValueClass(DoubleWritable.class);


        FileInputFormat.addInputPath(job, input);
        FileOutputFormat.setOutputPath(job, output);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    public static class MapperMinMaxPrice extends Mapper<LongWritable, Text, HLAWritable, DoubleWritable> {
        private boolean isFirstLine = true;
        private final HLAWritable hlaWritable = new HLAWritable();

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String linha = value.toString();
            String[] fields = linha.split(";");

            if (isFirstLine) {
                isFirstLine = false;
                return;
            }

            if (fields.length == 10 && !fields[0].isEmpty() && !fields[1].isEmpty() && !fields[8].isEmpty()) {
                String country = fields[0];
                String yearStr = fields[1];
                String amountStr = fields[8];


                int year = Integer.parseInt(yearStr);
                double amount = Double.parseDouble(amountStr);

                hlaWritable.setYear(year);
                hlaWritable.setCountry(country);
                context.write(hlaWritable, new DoubleWritable(amount));


            }
        }
    }

    public static class ReducerMinMaxPrice extends Reducer<HLAWritable, DoubleWritable, HLAWritable, Text> {
        protected void reduce(HLAWritable key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
            double minPrice = Double.MAX_VALUE;
            double maxPrice = Double.MIN_VALUE;

            for (DoubleWritable value : values) {
                double amount = value.get();

                // Atualiza minPrice se o valor atual for menor
                if (amount < minPrice) {
                    minPrice = amount;
                }

                // Atualiza maxPrice se o valor atual for maior
                if (amount > maxPrice) {
                    maxPrice = amount;
                }
            }

            String result = String.format("Min: %.2f, Max: %.2f", minPrice, maxPrice);

            context.write(key, new Text(result));
        }
    }
}