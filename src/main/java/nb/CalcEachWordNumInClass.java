package nb;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.Arrays;
import java.util.Vector;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by Yang Liu on 2018/12/29
 */

// 计算每个类中每个单词出现的次数
public class CalcEachWordNumInClass extends Configured implements Tool {
    public static class CalcEachWordNumInClassMapper
            extends Mapper<Text, BytesWritable, Text, IntWritable> {
        // 英文正则
        private static final Pattern PATTERN = Pattern.compile("[/sa-zA-Z]+");
        // 记录的单词
        private Text word = new Text();
        // 记录出现的次数
        private IntWritable singleCount = new IntWritable(1);
        // 停用词表
        private static String[] stopWordsArray = {"A", "a", "the", "an", "in",
                "on", "and", "The", "As", "as", "AND"};
        private static Vector<String> stopWords;

        @Override
        protected void setup(Context context) throws IOException,
                InterruptedException {
            if (null == stopWords) {
                stopWords = new Vector<>();
                //将停用词表中的单词加入到容器中
                stopWords.addAll(Arrays.asList(stopWordsArray));
            }
            super.setup(context);
        }

        public void map(Text key, BytesWritable value, Context context)
                throws IOException, InterruptedException {
            // 因为sequenceFile中的value是以二进制形式存储的，所以使用getBayes()方法将
            // 二进制的值转换为字符串
            String content = new String(value.getBytes(), 0, value.getLength());
            Matcher m = PATTERN.matcher(content);
            String[] classAndFile = key.toString().split("@");
            String className = classAndFile[0];

            while (m.find()) {
                String tempkey = m.group(); // 使用正则表达式，如果是英文单词则有效
                if (!stopWords.contains(tempkey)) {
                    // 如果不是停用词表中的词则有效
                    this.word.set(className + "@" + tempkey);
                    context.write(this.word, this.singleCount);
                }
            }
        }
    }

    public static class CalcEachWordNumInClassReducer
            extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable sumOfEachWordInClass = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> value, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable temp : value) {
                sum += temp.get();
            }
            this.sumOfEachWordInClass.set(sum);
            context.write(key, this.sumOfEachWordInClass);
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        if (2 != otherArgs.length) {
        }

        Path outputPath = new Path(Config.EACH_WORD_NUM_IN_CLASS_OUTPUT_PATH);
        FileSystem fs = outputPath.getFileSystem(conf);

        if (fs.exists(outputPath)) {
            fs.delete(outputPath, true);
        }

        Job job = Job.getInstance(conf, "CalcEachWordNumInClass");

        job.setJarByClass(CalcEachWordNumInClass.class);
        job.setMapperClass(CalcEachWordNumInClassMapper.class);
        job.setCombinerClass(CalcEachWordNumInClassReducer.class);
        job.setReducerClass(CalcEachWordNumInClassReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        SequenceFileInputFormat.addInputPath(job, new Path(Config.SEQUENCE_INPUT_TRAIN_DATA));
        SequenceFileOutputFormat.setOutputPath(job, outputPath);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new CalcEachWordNumInClass(), args);
        System.exit(res);
    }
}
