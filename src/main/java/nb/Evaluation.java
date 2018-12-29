package nb;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.reduce.IntSumReducer;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by Yang Liu on 2018/12/29
 */
public class Evaluation extends Configured implements Tool {
    private static HashMap<String, String> realClass = new HashMap<>();
    private static HashMap<String, String> predictClass = new HashMap<>();

    public HashMap<String, String> getRealClass(Configuration conf) throws IOException {
        String filePath = Config.RESULT_OF_CLASSFICATION + "/part-r-00000";
        SequenceFile.Reader reader = null;
        SequenceFile.Reader.Option pathOption = SequenceFile.Reader.file(new Path(filePath));
        try {
            reader = new SequenceFile.Reader(conf, pathOption);
            Text key = (Text) ReflectionUtils.newInstance(reader.getKeyClass(), conf);
            Text value = (Text) ReflectionUtils.newInstance(reader.getValueClass(), conf);
            while (reader.next(key, value)) {
                realClass.put(key.toString().split("@")[1], key.toString().split("@")[0]);
            }
        } finally {
            IOUtils.closeStream(reader);
        }
        return realClass;
    }

    public HashMap<String, String> getPredictClass(Configuration conf) throws IOException {
        String filePath = Config.RESULT_OF_CLASSFICATION + "/part-r-00000";
        SequenceFile.Reader reader = null;
        SequenceFile.Reader.Option pathOption = SequenceFile.Reader.file(new Path(filePath));
        try {
            reader = new SequenceFile.Reader(conf, pathOption);
            Text key = (Text) ReflectionUtils.newInstance(reader.getKeyClass(), conf);
            Text value = (Text) ReflectionUtils.newInstance(reader.getValueClass(), conf);
            while (reader.next(key, value)) {
                predictClass.put(key.toString().split("@")[1], value.toString().split("/")[0]);
            }
        } finally {
            IOUtils.closeStream(reader);
        }
        return predictClass;
    }

    public static class EvaluationMapper extends Mapper<Text, Text, Text, IntWritable> {
        private static final String tp = ":TP";
        private static final String tn = ":TN";
        private static final String fp = ":FP";
        private static final String fn = ":FN";
        private Text text = new Text();
        private static final IntWritable one = new IntWritable(1);
        private String currentClass = "";

        @Override
        public void map(Text key, Text value, Context context)
                throws IOException, InterruptedException {
            String className = key.toString().split("@")[0];
            // 出现过的类别，就进行混淆矩阵的计算
            if (!this.currentClass.equals(className)) {
                currentClass = className;
                System.out.println(currentClass);
                for (Map.Entry<String, String> entry : predictClass.entrySet()) {
                    if (realClass.get(entry.getKey()).equals(className)
                            && entry.getValue().equals(className)) {
                        // TN
                        text.set(className + tp);
                        context.write(text, one);
                    } else if (realClass.get(entry.getKey()).equals(className)
                            && !entry.getValue().equals(className)) {
                        // FN
                        text.set(className + fn);
                        context.write(text, one);
                    } else if (!realClass.get(entry.getKey()).equals(className)
                            && entry.getValue().equals(className)) {
                        // FP
                        text.set(className + fp);
                        context.write(text, one);
                    } else if (!realClass.get(entry.getKey()).equals(className)
                            && !entry.getValue().equals(className)) {
                        // TN
                        text.set(className + tn);
                        context.write(text, one);
                    }
                }
            }
        }
    }


    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = new Configuration();
        getRealClass(conf);
        getPredictClass(conf);

        Path outputPath = new Path(Config.EVALUATION_OUTPUT_PATH);
        FileSystem fs = outputPath.getFileSystem(conf);
        if (fs.exists(outputPath)) {
            fs.delete(outputPath, true);
        }

        Job job = Job.getInstance(conf, "Evaluation");
        job.setJarByClass(Evaluation.class);
        job.setMapperClass(EvaluationMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(Config.RESULT_OF_CLASSFICATION));
        FileOutputFormat.setOutputPath(job, outputPath);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new Evaluation(), args);
        System.exit(res);
    }
}
