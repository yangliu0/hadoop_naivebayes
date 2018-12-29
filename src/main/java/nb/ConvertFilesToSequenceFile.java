package nb;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.io.PrintWriter;

/**
 * Created by Yang Liu on 2018/12/10
 */

// 序列化训练集和测试集的文件
public class ConvertFilesToSequenceFile extends Configured implements Tool {
    public static class SequnceMapper
            extends Mapper<NullWritable, BytesWritable, Text, BytesWritable> {
        private Text filenameKey = new Text();

        @Override
        protected void setup(Context context) {
            InputSplit split = context.getInputSplit();
            String fileName = ((FileSplit) split).getPath().getName();
            String className = ((FileSplit) split).getPath().getParent().getName();
            filenameKey.set(className + "@" + fileName);
        }

        @Override
        public void map(NullWritable key, BytesWritable value, Context context)
                throws IOException, InterruptedException {
            context.write(filenameKey, value);
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();

        PrintWriter out = null;

        Path inputPath = new Path(conf.get("INPUTPATH"));
        Path outputPath = new Path(conf.get("OUTPUTPATH"));

        FileSystem trainFile = inputPath.getFileSystem(conf);
        FileStatus[] trainFileList = trainFile.listStatus(inputPath);
        String[] INPUT_TRAIN_PATH = new String[trainFileList.length];

        boolean flag = true;

        for (int i = 0; i < trainFileList.length; i++) {
            INPUT_TRAIN_PATH[i] = trainFileList[i].getPath().toString();
            if (conf.get("INPUTPATH").equals(Config.BASE_TRAIN_DATA_PATH)) {
                if (flag) {
                    out = new PrintWriter(Config.LOG_FILE);
                    flag = false;
                }
                out.print(trainFileList[i].getPath().getName() + "/");
            }
            System.out.println("document: " + INPUT_TRAIN_PATH[i]);
        }

        FileSystem fs = outputPath.getFileSystem(conf);
        if (fs.exists(outputPath)) {
            fs.delete(outputPath, true);
        }

        Job job = Job.getInstance(conf, "ConverFilesToSequenceFile");

        job.setJarByClass(ConvertFilesToSequenceFile.class);
        job.setMapperClass(SequnceMapper.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(BytesWritable.class);
        job.setInputFormatClass(WholeFileInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        if (null != INPUT_TRAIN_PATH) {
            for (String path : INPUT_TRAIN_PATH) {
                WholeFileInputFormat.addInputPath(job, new Path(path));
            }
        }


        SequenceFileOutputFormat.setOutputPath(job, outputPath);

        if (!flag) {
            out.close();
        }

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new Configuration(), new ConvertFilesToSequenceFile(), args);
        System.exit(exitCode);
    }
}
