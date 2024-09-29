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

public class BrazilCount {
    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure();
        Configuration conf = new Configuration(); //configuração hadoop

        //com essa linha na hora de rodar eu tenho q passar os caminhos de entrada e saida.
        // in/operacoes_comerciais_inteira.csv output/teste_resultado.txt
        String[] files = new GenericOptionsParser(conf, args).getRemainingArgs();

        //aqui eh onde ele vai receber os caminhos
        Path input = new Path(files[0]);
        Path output = new Path(files[1]);

        //configura uma nova instância de um trabalho MapReduce no Hadoop
        Job job = Job.getInstance(conf, "BrazilCount");

        // registro das classes
        job.setJarByClass(BrazilCount.class);//nome da classe q esta a main
        job.setMapperClass(MapBrazilCount.class);//classe mapper
        job.setReducerClass(ReduceBrazilCount.class);//reducer classe

        //tipos das saidas
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, input);
        FileOutputFormat.setOutputPath(job, output);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
    public static class MapBrazilCount extends Mapper<LongWritable, Text, Text, IntWritable>{
        private boolean isFirstLine = true;

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
            String linha = value.toString();

            //separa a linha em campos
            String[] fields = linha.split(";");

            if (isFirstLine){
                isFirstLine = false;
                return;
            }

            if (fields.length == 10 && fields[0].equals("Brazil")){
                context.write(new Text("Brazil"), new IntWritable(1));
            }
        }
    }
    public static class ReduceBrazilCount extends Reducer<Text, IntWritable, Text, IntWritable> {


        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws  IOException, InterruptedException{
            int soma = 0;
            for (IntWritable valor: values){

                soma += valor.get();
            }

            context.write(key, new IntWritable(soma));
        }
    }
}
