package nb;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
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
 * Created by Yang Liu on 2018/12/10
 */
public class CalcDocNumInClass extends Configured implements Tool {
    public static class CalcDocNumInClassMapper
            extends Mapper<Text, BytesWritable, Text, Text> {
        private Text className = new Text();
        private Text value = new Text();

        public void map(Text key, BytesWritable value, Context context)
                throws IOException, InterruptedException {
            // 获取文件名和文件目录名
            String[] classAndFile = key.toString().split("@");
            System.out.println(classAndFile[0]);
            String className = classAndFile[0];

            this.className.set(className);
            this.value.set("1");
            context.write(this.className, this.value);
        }
    }

    public static class CalcDocNumInClassReducer
            extends Reducer<Text, Text, Text, Text> {
        private Text className = new Text();
        private Text docNumInClass = new Text();

        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            for (Text tmpValue : values) {
                String tmp = tmpValue.toString();
                sum += Integer.parseInt(tmp);
            }
            this.className.set(key);
            this.docNumInClass.set(String.valueOf(sum));
            context.write(this.className, this.docNumInClass);
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        Path outputPath = new Path(Config.DOC_NUM_IN_CLASS_OUTPUT_PATH);
        FileSystem fs = outputPath.getFileSystem(conf);
        if (fs.exists(outputPath)) {
            fs.delete(outputPath, true);
        }

        Job job = Job.getInstance(conf, "CalDocNumInClass");

        job.setJarByClass(CalcDocNumInClass.class);
        job.setMapperClass(CalcDocNumInClassMapper.class);
        job.setCombinerClass(CalcDocNumInClassReducer.class);
        job.setReducerClass(CalcDocNumInClassReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        SequenceFileInputFormat.addInputPath(job, new Path(Config.SEQUENCE_INPUT_TRAIN_DATA));

        SequenceFileOutputFormat.setOutputPath(job, outputPath);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new CalcDocNumInClass(), args);
        System.exit(res);
    }

}
