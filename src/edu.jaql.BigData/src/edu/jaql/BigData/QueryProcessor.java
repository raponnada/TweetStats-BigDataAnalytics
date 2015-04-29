package edu.jaql.BigData;

import java.io.BufferedReader;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.URL;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.lang.JaqlQuery;

import edu.jaql.BigData.WordCount.Map;
import edu.jaql.BigData.WordCount.Reduce;

public class QueryProcessor {
	
	public static void main(String[] args) { 
	     if (args.length != 4) { 
	       System.err.println("Usage: WriteFS2HDFS <local_in> <hdfs_out>"); 
	       System.exit(2); 
	     } 
	try{ 

		QueryProcessor query = new QueryProcessor(); 
		query.runQuery(args);
	
		//Creating a JobConf object and assigning a job name for identification purposes
		JobConf conf = new JobConf(WordCount.class);
		conf.setJobName("wordcount");

		//Setting configuration object with the Data Type of output Key and Value
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(IntWritable.class);
		//Providing the mapper and reducer class names
		conf.setMapperClass(Map.class);
		conf.setReducerClass(Reduce.class);

		//Setting format of input and output
		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

		//The hdfs input and output directory to be fetched from the command line
		FileInputFormat.setInputPaths(conf, new Path(args[2]));
		FileOutputFormat.setOutputPath(conf, new Path(args[3]));

		//Running the job
		JobClient.runJob(conf);

	}catch(Exception ex){ 
	ex.printStackTrace(); 
	} 
	}
	
	private void runQuery(String[] args) { 
		
	try { 
		JaqlQuery q = new JaqlQuery();    
		FileWriter tweetFile1,tweetFile2,tweetFile3,tweetFile4,tweetFile5 = null;
		
		//Read twitter stream data from Local file and store it in the HDFS
		q.setQueryString("read(file($srcfile)) -> write(hdfs($tgtfile));");                       
		q.setVar("$srcfile", args[0]);                   
		q.setVar("$tgtfile", args[1]); 
		q.evaluate();
		
		//Reading data from HDFS and getting the tweet count grouped by the created_at date and time
		q.setQueryString("read(hdfs($tgtfile)) -> group by d = $.created_at into {d, total: count($.created_at)};");
		JsonValue date = q.evaluate();
		
		//Reading data from HDFS and getting the coordinates of the top 10000 tweets
		q.setQueryString("read(hdfs($tgtfile)) -> transform $.coordinates.coordinates -> top 10000;");
		JsonValue loc = q.evaluate();
		
		//Reading data from HDFS and getting the top 10 Languages along with thier count
		q.setQueryString("read(hdfs($tgtfile)) -> group by l = $.lang into { l, total: count($.lang)}  -> top 10 by [$.total desc];");
		JsonValue lang = q.evaluate();
		
		//Reading data from HDFS and getting the text of all the tweets
		q.setQueryString("read(hdfs($tgtfile)) -> transform $.text;");
		JsonValue text = q.evaluate();
		
		//Reading data from HDFS and getting the profile image url of the top 10 celebrities
		q.setQueryString("read(hdfs($tgtfile)) -> transform {$.user.name, $.user.followers_count, $.user.profile_image_url} -> top 10 by [$.followers_count desc] -> transform $.profile_image_url;");
		JsonValue image = q.evaluate();
		
		q.close(); 
		
		String locData = loc.toString().replaceAll("[\n ]","").replaceAll("null,","").replaceAll("\\[\\[", "").replaceAll("\\]\\]", "").replaceAll("\\],\\[","\n");
		tweetFile1 = new FileWriter("/home/biadmin/project/location.txt", true);
	    PrintWriter tweetWriter1 = new PrintWriter(tweetFile1);
	    tweetWriter1.println(locData);
	    tweetFile1.flush();
	    tweetFile1.close();
	    
		String langData = lang.toString().replaceAll("[ \"\n\\]\\[]","").replaceAll("\\},\\{","\n").replaceAll("total:","").replaceAll("l:","").replaceAll("[{}]","");
		tweetFile2 = new FileWriter("/home/biadmin/project/language.txt", true);
		PrintWriter tweetWriter2 = new PrintWriter(tweetFile2);
	    tweetWriter2.println("language,count");
		tweetWriter2.println(langData);
	    tweetFile2.flush();
	    tweetFile2.close();

		String dateData = date.toString().replaceAll("[ \"+\n\\]\\[]","").replaceAll("\\},\\{","\n").replaceAll("total:","").replaceAll("d:","").replaceAll("[{}]","").replaceAll("00002014","");
		tweetFile3 = new FileWriter("/home/biadmin/project/date.txt", true);
	    PrintWriter tweetWriter3 = new PrintWriter(tweetFile3);
	    tweetWriter3.println("time,count");
	    tweetWriter3.println(dateData);
	    tweetFile3.flush();
	    tweetFile3.close();
	    
	    String textData = text.toString().replaceAll("[^a-zA-Z0-9#]"," ");
	    String expression = "#[a-zA-Z][a-zA-Z]*";
	    Pattern pattern = Pattern.compile(expression);
	    Matcher matcher = pattern.matcher(textData);
	    String hashData="";
	    while(matcher.find()){
	      hashData = hashData + " " + matcher.group();
	    }	    
	    hashData = hashData.replaceAll("#","");
	    tweetFile4 = new FileWriter("/home/biadmin/project/text.txt", true);
	    PrintWriter tweetWriter4 = new PrintWriter(tweetFile4);
	    tweetWriter4.println(hashData);
	    tweetFile4.flush();
	    tweetFile4.close();
	    
	    
	    String imageData = image.toString().replaceAll("[\"\n \\[\\]]","").replaceAll(",","\n");
	    tweetFile5 = new FileWriter("/home/biadmin/project/image.txt", true);
	    PrintWriter tweetWriter5 = new PrintWriter(tweetFile5);
	    tweetWriter5.println(imageData);
	    tweetFile5.flush();
	    tweetFile5.close();
	    
	    BufferedReader br = new BufferedReader(new FileReader("/home/biadmin/project/image.txt"));
	    String imageUrl="";
	    String destinationFile="";
	    int count=0;
	    while ((imageUrl = br.readLine()) != null) {
	    	System.out.println(imageUrl);
	    	count++;
	    	URL url = new URL(imageUrl);
	    	InputStream is = url.openStream();
	    	destinationFile = count+".jpg";
	    	OutputStream os = new FileOutputStream("/home/biadmin/project/"+destinationFile);

	    	byte[] b = new byte[2048];
	    	int length;

	    	while ((length = is.read(b)) != -1) {
	    		os.write(b, 0, length);
	    	}

	    	is.close();
	    	os.close();
	    }
	    
	  
	}catch(Exception ex){ 
		ex.printStackTrace();          
	} 
	} 
}
