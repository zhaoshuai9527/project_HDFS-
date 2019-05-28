package sanyuehuakai;

import java.io.IOException;
import java.util.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class Delect {
	public static void main(String[]args)throws IOException{
		
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
		System.out.print("请输入要删除的文件名或文件夹名:");
		p=sc.next();
		Path dir= new Path("hdfs://master/"+p);
		fs.delete(dir,false);
		System.out.println("删除完成...");
		fs.close();
		try{
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
		sc.close();
		}
		
}