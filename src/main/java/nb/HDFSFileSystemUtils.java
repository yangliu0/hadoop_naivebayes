package nb; /**
 * Created by Yang Liu on 2018/12/8
 */

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

public class HDFSFileSystemUtils {

    // 设置HDFS的URL和端口号
    private String HDFSURL;
    // HDFS的配置信息
    private Configuration conf;

    private FileSystem fs;

    public HDFSFileSystemUtils(String HDFSURL) {
        this.HDFSURL = HDFSURL;
        this.conf = new Configuration();
        try {
            // 得到FileSystem的连接
            this.fs = FileSystem.get(URI.create(HDFSURL), conf);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /*
     * 打开FileSystem
     */
    public void openFileSystem() {
        try {
            this.fs = FileSystem.get(URI.create(HDFSURL), conf);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 关闭FileSystem
     */
    public void closeFileSystem() {
        try {
            this.fs.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 上传本地文件到HDFS
     *
     * @param local 本地文件的目录
     * @param dst   HDFS中的目录，或者文件名
     */
    public void putFile(String local, String dst) {
        try {
            // 从本地将文件拷贝到HDFS中，如果目标文件已存在则进行覆盖
            dst = HDFSURL + dst;
            fs.copyFromLocalFile(new Path(local), new Path(dst));
            System.out.println("上传成功！");
            // 关闭连接
        } catch (IOException e) {
            System.out.println("上传失败！");
            e.printStackTrace();
        }
    }

    /**
     * 下载HDFS中的文件到本地
     *
     * @param dst   HDFS中文件目录
     * @param local 本地目录
     */
    public void getFile(String dst, String local) {
        try {
            dst = HDFSURL + dst;
            if (!fs.exists(new Path(dst))) {
                System.out.println("文件不存在！");
            } else {
                fs.copyToLocalFile(new Path(dst), new Path(local));
                System.out.println("下载成功！");
            }
        } catch (IOException e) {
            System.out.println("下载失败！");
            e.printStackTrace();
        }
    }

    /**
     * 删除HDFS中的文件或者目录
     *
     * @param dst
     */
    public void deleteFile(String dst) {
        try {
            dst = HDFSURL + dst;
            if (!fs.exists(new Path(dst))) {
                System.out.println("文件不存在！");
            } else {
                fs.delete(new Path(dst), true);
                System.out.println("删除成功！");
            }
        } catch (IOException e) {
            System.out.println("删除失败！");
            e.printStackTrace();
        }
    }

    /**
     * 显示HDFS中的文件到输入流中，显示在console中
     *
     * @param dst
     */
    public void catFile(String dst) {
        dst = HDFSURL + dst;
        FSDataInputStream in = null;
        try {
            if (!fs.exists(new Path(dst))) {
                System.out.println("文件不存在！");
            } else {
                System.out.println(dst + ": ");
                // 打开文件流
                in = fs.open(new Path(dst));
                IOUtils.copyBytes(in, System.out, 4096, false);
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            IOUtils.closeStream(in);
        }
    }

    /**
     * 显示出所传入目录下的所有文件
     *
     * @param dst
     */
    public List<Path> listStatus(String dst) {
        List<Path> paths = new ArrayList<>();
        try {
            dst = HDFSURL + dst;
            if (!fs.exists(new Path(dst))) {
                System.out.println("目录不存在！");
                return paths;
            }
            // 得到文件的状态
            FileStatus[] status = fs.listStatus(new Path(dst));
            for (FileStatus s : status) {
                paths.add(s.getPath());
                System.out.println(s.getPath());
            }

        } catch (IllegalArgumentException | IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return paths;
    }

    /**
     * 判断文件是否存在
     *
     * @param dst
     */
    public void isExists(String dst) {
        dst = HDFSURL + dst;
        try {
            if (fs.exists(new Path(dst))) {
                System.out.println("文件存在！");
            } else {
                System.out.println("文件不存在！");
            }
        } catch (IllegalArgumentException | IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 创建路径
     *
     * @param dst
     */
    public void createDir(String dst) {
        dst = HDFSURL + dst;
        try {
            if (fs.exists(new Path(dst))) {
                System.out.println("目录已存在！");
            } else {
                fs.mkdirs(new Path(dst));
                System.out.println("创建成功！");
            }
        } catch (IllegalArgumentException | IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 创建文件，并且可以将制定内容写到文件中，如果为null则创建一个空的文件
     *
     * @param dst
     * @param content
     */
    public void createFile(String dst, String content) {
        dst = HDFSURL + dst;
        try {
            if (fs.exists(new Path(dst))) {
                System.out.println("文件已存在！");
            } else {
                Path path = new Path(dst);
                // 创建一个空文件
                if (content.equals("") || content == null) {
                    fs.createNewFile(path);
                    System.out.println("创建了一个空文件！");
                } else {
                    // 创建一个文件，然后将内容写入进去
                    FSDataOutputStream out = fs.create(path);
                    out.write(content.getBytes());
                    out.close();
                    System.out.println("文件创建成功，内容已写入！");
                }
            }
        } catch (IllegalArgumentException | IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 将dst1重命名为dst2，也可以进行文件的移动
     *
     * @param dst1
     * @param dst2
     */
    public void moveFile(String dst1, String dst2) {
        dst1 = HDFSURL + dst1;
        Path path1 = new Path(dst1);
        dst2 = HDFSURL + dst2;
        Path path2 = new Path(dst2);

        try {
            if (!fs.exists(path1)) {
                System.out.println(dst1 + " 文件不存在！");
                return;
            }
            if (fs.exists(path2)) {
                System.out.println(dst2 + "已存在！");
                return;
            }
            // 将文件进行重命名，可以起到移动文件的作用
            fs.rename(path1, path2);
            System.out.println("文件已重命名！");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 追加content 到dst所指的文件中
     *
     * @param dst
     * @param content
     */
    public void appendFile(String dst, String content) {
        dst = HDFSURL + dst;
        Path path = new Path(dst);
        try {
            if (!fs.exists(path)) {
                System.out.println("文件不存在！");
            } else {
                InputStream in = new ByteArrayInputStream(content.getBytes());
                FSDataOutputStream out = fs.append(path);
                IOUtils.copyBytes(in, out, 4096, true);
                System.out.println("文件追加成功！");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 将HDFS上文件复制到HDFS上
     *
     * @param src 原目标
     * @param dsc 复制到的目标
     */
    public void copy(String src, String dsc) {
        src = HDFSURL + src;
        dsc = HDFSURL + dsc;
        try {
            //建立输入流
            FSDataInputStream input = fs.open(new Path(src));

            //建立输出流
            FSDataOutputStream output = fs.create(new Path(dsc));

            //两个流的对接
            byte[] b = new byte[1024];
            int hasRead = 0;
            while ((hasRead = input.read(b)) > 0) {
                output.write(b, 0, hasRead);
            }
            input.close();
            output.close();
        } catch (IllegalArgumentException | IOException e) {
            e.printStackTrace();
        }

    }

    public static void main(String[] args) {
        HDFSFileSystemUtils fsUtils = new HDFSFileSystemUtils("hdfs://192.168.1.109:9001");
        fsUtils.deleteFile("/input/train");
        fsUtils.deleteFile("/input/test");
//        List<Path> res = fsUtils.listStatus("/input/test/AUSTR");
//        System.out.println("test");
//        System.out.println(res.size());
//        for (Path path : res) {
//            System.out.println(path.toString().substring(path.toString().lastIndexOf('/')));
//        }
    }
}
