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


public class AvarageBrazilTransactionsPerYear {
    public static void main(String[] args) throws Exception{
        BasicConfigurator.configure();
        Configuration conf = new Configuration();

        String[] files = new GenericOptionsParser(conf, args).getRemainingArgs();

        Job job = Job.getInstance(conf, "TransactionsPerYear");

        Path input = new Path(files[0]);
        Path output = new Path(files[1]);

        job.setJarByClass(AvarageBrazilTransactionsPerYear.class);
        job.setMapperClass(MapperAvarageBrazilTransactionsPerYear.class);
        job.setReducerClass(ReducerAvarageBrazilTransactionsPerYear.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        FileInputFormat.addInputPath(job, input);
        FileOutputFormat.setOutputPath(job, output);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
    public static class MapperAvarageBrazilTransactionsPerYear extends Mapper<LongWritable, Text, Text, DoubleWritable>{
        private boolean isFirstLine = true;

        public void map(LongWritable key, Text value, Context context)throws IOException, InterruptedException{
            String linha = value.toString();

            String[] fields = linha.split(";");


            if (isFirstLine) {
                isFirstLine = false;
                return;
            }

            if (fields.length == 10 && fields[0].equals("Brazil") && !fields[1].isEmpty() && !fields[5].isEmpty()) {
                String year = fields[1];
                double transactionValue = Double.parseDouble(fields[5]);
                context.write(new Text(year), new DoubleWritable(transactionValue));
            }
        }
    }
    public static class ReducerAvarageBrazilTransactionsPerYear extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
        private DoubleWritable result = new DoubleWritable();

        protected void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
            double sum = 0;
            int count = 0;

            for (DoubleWritable value : values) {
                sum += value.get();
                count++;
            }

            double average = sum / count;
            result.set(average);
            context.write(key, result);
        }
    }
}
