package sanyuehuakai;
import java.io.IOException;
import java.util.Scanner;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.zookeeper.common.IOUtils;

public class Select {
    public static void main(String[] args) throws IOException {
           Scanner sc=new Scanner(System.in);
           Configuration conf=new Configuration();

           conf.set("fs.defaultFS", "hdfs://master");
           conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
           FileSystem fs=FileSystem.get(conf);
           try{

               FileSystem hdfs=FileSystem.get(conf);
             Path path=new Path("hdfs://master/");
             FileStatus status[]=hdfs.listStatus(path);
             for(int i=0; i<status.length;i++){
             System.out.println(status[i].getPath().toString());
             }

             }
             catch(Exception e){
             e.printStackTrace();
             }
             String p;
             System.out.print("请输入要查看的文件名:");
             p=sc.next();
             Path path=new Path("hdfs://master/"+p);
             if(fs.exists(path))
             {
                try{
                     FSDataInputStream is=fs.open(path);
                
                     IOUtils.copyBytes(is,System.out,1024,false);
                }catch(Exception e){
                     IOUtils.closeStream(fs);
           }
      }
      
    }
}
