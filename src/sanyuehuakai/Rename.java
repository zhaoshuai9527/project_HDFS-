package sanyuehuakai;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import java.util.*;
import org.apache.hadoop.fs.FileStatus;

public class Rename{
    FileSystem fs =null;

    public void init() throws IOException,URISyntaxException{
       fs= FileSystem.get(new URI("hdfs://master"), new Configuration()); }
    public static void main(String[] args) throws IOException, URISyntaxException{
    	Scanner sc=new Scanner(System.in);
    	String p;
    	String q;
       try{Configuration conf=new Configuration();
           conf.set("fs.defaultFS", "hdfs://master/");
           conf.set("fs.hdfs.impl","org.apache.hadoop.hdfs.DistributedFileSystem");
           FileSystem hdfs=FileSystem.get(conf);
           Path path=new Path("hdfs://master/");
           FileStatus status[]=hdfs.listStatus(path);
           for(int i=0;i<status.length;i++){
        	   System.out.println(status[i].getPath().toString());}  
           hdfs.close();}
       catch(Exception e){
       e.printStackTrace();}
      FileSystem  fs = FileSystem.get(new  URI("hdfs://master/"), new Configuration());
      
      System.out.print("请输入待重命名文件:");
      p=sc.next();
      System.out.print("请输入文件重命名:");
      q=sc.next();
      fs.rename(new Path("hdfs://master/"+p),new Path("hdfs://master//"+q));
      System.out.println("重命名完成...");
      try{Configuration conf=new Configuration();
             conf.set("fs.defaultFS","hdfs://master/");
             conf.set("fs.hdfs.impl","org.apache.hadoop.hdfs.DistributedFileSystem");
             FileSystem hdfs=FileSystem.get(conf);
         Path path=new Path("hdfs://master/");
FileStatus status[]=hdfs.listStatus(path);
for(int i=0;i<status.length;i++){
System.out.println(status[i].getPath().toString());
}
hdfs.close();
}
catch(Exception e){
e.printStackTrace();
}

}
}
