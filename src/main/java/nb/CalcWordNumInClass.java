package nb;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 * Created by Yang Liu on 2018/12/29
 */

// 计算每个类中总共单词数
public class CalcWordNumInClass extends Configured implements Tool {
    public static class CalcWordNumInClassMapper extends
            Mapper<Text, IntWritable, Text, IntWritable> {
        private Text className = new Text();

        public void map(Text key, IntWritable value, Context context)
                throws IOException, InterruptedException {
            // 输出的key格式为 className@word
            String[] classAndWord = key.toString().split("@");
            String className = classAndWord[0];
            this.className.set(className);
            context.write(this.className, value);
        }
    }

    public static class CalcWordNumInClassReducer extends
            Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable sumOfWordInClass = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> value, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable temp : value) {
                sum += temp.get();
            }
            this.sumOfWordInClass.set(sum);
            context.write(key, this.sumOfWordInClass);
        }
    }


    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();

        Path outputPath = new Path(Config.ALL_WORD_NUM_IN_CLASS_OUTPUT_PATH);
        FileSystem fs = outputPath.getFileSystem(conf);
        if (fs.exists(outputPath)) {
            fs.delete(outputPath, true);
        }

        Job job = Job.getInstance(conf, "CalcWordNumInClass");

        job.setJarByClass(CalcWordNumInClass.class);
        job.setMapperClass(CalcWordNumInClassMapper.class);
        job.setCombinerClass(CalcWordNumInClassReducer.class);
        job.setReducerClass(CalcWordNumInClassReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        SequenceFileInputFormat.addInputPath(job, new Path(Config.EACH_WORD_NUM_IN_CLASS_OUTPUT_PATH));
        SequenceFileOutputFormat.setOutputPath(job, outputPath);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new CalcWordNumInClass(), args);
        System.exit(res);
    }
}
