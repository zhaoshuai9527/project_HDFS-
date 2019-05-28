package sanyuehuakai;

import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.junit.Before;
import java.util.*;
import org.apache.hadoop.fs.FileStatus;

public class Download {
	
	FileSystem fs = null;

	@Before
	public void init() throws Exception{
		fs = FileSystem.get(new URI("hdfs://master"), new Configuration());
	}

	public static void main(String[] args)throws Exception{
    Scanner sc=new Scanner(System.in);
    String p;
    String q;
    try{Configuration conf=new Configuration();
            conf.set("fs.defaultFS","hdfs://master");
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

	e.printStackTrace();}

	FileSystem fs = FileSystem.get(new URI("hdfs://master/"),
			new Configuration());System.out.print("请输入待下载的文件名或文件夹名:");
			p=sc.nextLine();
	InputStream in = fs.open(new Path("hdfs://master/" + p));
	System.out.print("请输入文件的保存位置:");
	q=sc.nextLine();
	OutputStream out;
	if(q.length()==0)

	{
		out = new FileOutputStream("/home/sanyuehuakai/Downloads/" + p);
		IOUtils.copyBytes(in, out, 4096, true);
		System.out.println("下载完成...");
	} else

	{
		
		out = new FileOutputStream(q+"/"+p);
		IOUtils.copyBytes(in, out, 4096, true);
		System.out.println("下载完成...");
	}
	sc.close();
	}
	}
