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

public class TransactionsPerFlow {
    public static void main(String[] args) throws Exception{
        BasicConfigurator.configure();
        Configuration conf = new Configuration();

        String[] files = new GenericOptionsParser(conf, args).getRemainingArgs();

        Job job = Job.getInstance(conf, "TransactionsPerFlow");

        Path input = new Path(files[0]);
        Path output = new Path(files[1]);

        job.setJarByClass(TransactionsPerFlow.class);
        job.setMapperClass(MapperTransactionsPerFlow.class);
        job.setReducerClass(ReducerTransactionsPerFlow.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, input);
        FileOutputFormat.setOutputPath(job, output);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
    public static class MapperTransactionsPerFlow extends Mapper<LongWritable, Text, Text, IntWritable>{
        private boolean isFirstLine = true;

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
            String linha = value.toString();

            String[] fields = linha.split(";");

            if (isFirstLine){
                isFirstLine = false;
                return;
            }

            if (fields.length == 10 && !fields[4].isEmpty()) {
                String flow = fields[4];//category
                context.write(new Text(flow), new IntWritable(1));
            }
        }
    }
    public static class ReducerTransactionsPerFlow extends Reducer<Text, IntWritable, Text, IntWritable> {

        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable value : values) {
                sum += value.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }
}
