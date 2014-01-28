/*
 *author@Utkarsh
 *
 */
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapred.lib.MultipleOutputs;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class KMeans{
	public static String input_dir = "/user/utkarsh/input";
	public static String output_dir = "/user/utkarsh/output";
	public static String output_file_name = "/part-00000";
	public static int clusters;
	public static int exprSize;
	public static double newMean = 0;
	public static double oldMean = 0;
	public static HashMap<Integer,ArrayList<Double>> map = new HashMap<Integer,ArrayList<Double>>();
	public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, IntWritable, Text> {
		public static HashMap<Integer,ArrayList<Double>> centers = new HashMap<Integer,ArrayList<Double>>();
		@Override
		public void configure(JobConf job) {
			try {
				Path[] cacheFiles = DistributedCache.getLocalCacheFiles(job);
				if (cacheFiles != null && cacheFiles.length > 0){
					BufferedReader reader = new BufferedReader(new FileReader(cacheFiles[0].toString()));
					String line;
					while((line=reader.readLine())!=null){
						int id;
						ArrayList<Double> temp= new ArrayList<Double>();
						String[] tokens = line.split("\t");
						id = Integer.parseInt(tokens[0]);
						for(int i = 1;i<tokens.length;i++){
							temp.add(Double.parseDouble(tokens[i]));
						}
						centers.put(id, temp);
					}
					reader.close();
				}
				else{
					System.out.println("No Cache files");
				}
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		@Override
		public void map(LongWritable key, Text value, OutputCollector<IntWritable, Text> collector, Reporter reporter) throws IOException {
			// TODO Auto-generated method stub
			String line = value.toString();
			String[] tokens = line.split("\t");
			double min = Double.MAX_VALUE;
			int nearest=1;
			String exp =tokens[0]+"\t"+tokens[2];
			ArrayList<Double> b = new ArrayList<Double>();
			b.add(Double.parseDouble(tokens[2]));
			for(int i = 3;i<tokens.length;i++){
				exp = exp+"\t"+tokens[i];
				Double next = Double.parseDouble(tokens[i]);
				b.add(next);
			}
			
			for(int i : centers.keySet()){
				ArrayList<Double> a = new ArrayList<Double>();
				a = centers.get(i);
				double sum = 0;
				double dist = 0;
				for(int j=0;j<a.size();j++){
					sum = sum+Math.pow((a.get(j)-b.get(j)), 2);
				}
				dist = Math.sqrt(sum);
				//}
				if(dist<min){
					min = dist;
					nearest = i;
				}
			}
			collector.collect(new IntWritable(nearest), new Text(exp));
		}
	}
	public static class Reduce extends MapReduceBase implements Reducer<IntWritable, Text, IntWritable, Text>{

		public MultipleOutputs mos;
		private int x;
		@Override
		public void configure(JobConf job) {
			mos = new MultipleOutputs(job);
			x = Integer.parseInt(job.get("expr"));
		}
		@Override
		public void reduce(IntWritable key, Iterator<Text> value, OutputCollector<IntWritable, Text> collector, Reporter reporter)throws IOException {
			// TODO Auto-generated method stub
			int n = 0;
			double[] sum = new double[x];
			while(value.hasNext()){
				String exp = value.next().toString();
				//System.out.println("Received: "+exp);
				String[] vals = exp.split("\t");
				//int id = (Integer.parseInt(vals[0]));
				String id = vals[0];
				ArrayList<Double> temp = new ArrayList<Double>();
				for(int i = 1;i<vals.length;i++){
					Double d = Double.parseDouble(vals[i]);
					temp.add(d);
				}
				//System.out.println("Temp: "+temp.get(0)+temp.get(1));
				for(int i = 0;i<x;i++){
					sum[i] += temp.get(i);
				}
				n++;
				//gid.add(id);
				mos.getCollector("text", reporter).collect(key, new Text(id));
			}
			for(int i = 0;i<sum.length;i++){
				sum[i] = sum[i]/n;
			}
			String output = Double.toString(sum[0]);
			for(int i = 1;i<sum.length;i++){
				output = output+"\t"+Double.toString(sum[i]);
			}
			//output = output+"\n";
			//info.add(gid);
			collector.collect(key, new Text(output));
		}
		@Override
		public void close() throws IOException {
			mos.close();
		}
	}
	
	public static void main(String[] args) throws IOException{
		run(args);
	}
	public static void run(String[] args) throws IOException{
		String inFile = args[0];
		String number = args[1];
		String expr = args[2];
		clusters = Integer.parseInt(number);
		exprSize = Integer.parseInt(expr);
		//System.out.println("Expr: "+exprSize);
		boolean done = false;
		int iteration = 0;
		Configuration config = new Configuration();
		FileSystem fs = FileSystem.get(config);
		Path filename = new Path(input_dir+inFile);
		Path centroid = new Path("/user/utkarsh/centroid.txt");
		if(fs.exists(centroid)){
			fs.delete(centroid, true);
		}
		BufferedWriter out = new BufferedWriter(new OutputStreamWriter(fs.create(centroid))); 
		BufferedReader in = new BufferedReader(new InputStreamReader(fs.open(filename)));
		String line;
		ArrayList<String> list = new ArrayList<String>();
		while((line = in.readLine())!=null){
			String[] tokens = line.split("\t");
			int id = Integer.parseInt(tokens[0]);
			ArrayList<Double> values = new ArrayList<Double>();
			for(int i = 2;i<tokens.length;i++){
				values.add(Double.parseDouble(tokens[i]));
			}
			map.put(id, values);
			String[] val = line.split("\t",3);
			list.add(val[2]);
		}
		in.close();
		Random generator = new Random();
		for(int i = 1;i<clusters+1;i++){
			int num = generator.nextInt(list.size());
			String text = Integer.toString(i)+"\t"+list.get(num)+"\n";	//check
			out.write(text);
		}
		out.close();
		String output = output_dir+Integer.toString(iteration);
		long startTime = System.currentTimeMillis();
		while(done==false){
			oldMean = newMean;
			HashMap<Integer,ArrayList<Double>> centroids = new HashMap<Integer,ArrayList<Double>>();
			System.out.println("Iteration: "+iteration);
			BufferedReader reader;
			JobConf conf = new JobConf(KMeans.class);
			if (iteration == 0){
				DistributedCache.addCacheFile(centroid.toUri(), conf);
				reader = new BufferedReader(new InputStreamReader(fs.open(centroid)));
			}
			else{
				Path hdfsPath = new Path(output+output_file_name);
				DistributedCache.addCacheFile(hdfsPath.toUri(), conf);
				reader = new BufferedReader(new InputStreamReader(fs.open(hdfsPath)));
			}
			output = output_dir+Integer.toString(iteration);
			while((line=reader.readLine())!=null){
				int id;
				ArrayList<Double> temp= new ArrayList<Double>();
				String[] tokens = line.split("\t| ");
				id = Integer.parseInt(tokens[0]);
				for(int i = 1;i<tokens.length;i++){
					temp.add(Double.parseDouble(tokens[i]));
				}
				centroids.put(id, temp);
			}
			reader.close();
			conf.setJobName("KMeans");
            conf.setMapOutputKeyClass(IntWritable.class);
            conf.setMapOutputValueClass(Text.class);
            conf.setOutputKeyClass(IntWritable.class);
            conf.setOutputValueClass(Text.class);
            conf.setMapperClass(Map.class);
            conf.setReducerClass(Reduce.class);
            conf.setInputFormat(TextInputFormat.class);
            conf.setOutputFormat(TextOutputFormat.class);
            conf.set("expr", Integer.toString(exprSize));
            FileInputFormat.setInputPaths(conf, new Path(input_dir + inFile));
            FileOutputFormat.setOutputPath(conf, new Path(output));
            MultipleOutputs.addNamedOutput(conf,"text",TextOutputFormat.class,IntWritable.class,Text.class);
            JobClient.runJob(conf);
            newMean = calculateMean(centroids);
            if(oldMean==newMean)
            	done = true;
            else
            	++iteration;
		}
		long stopTime = System.currentTimeMillis();
		System.out.println("Time: "+(stopTime-startTime));
	}
	public static Double calculateMean(HashMap<Integer,ArrayList<Double>> c){
		ArrayList<ArrayList<Double>> values = new ArrayList<ArrayList<Double>>(); 
		for(int i = 1;i<=c.size();i++){
			values.add(c.get(i));
		}
		Iterator<ArrayList<Double>> it = values.iterator();
		double sum = 0;
		while(it.hasNext()){
			ArrayList<Double> temp = it.next();
			//size = temp.size();
			for(int i = 0;i<exprSize;i++){
				sum+=temp.get(i);
			}
		}
		return (sum/(exprSize*(c.size())));
	}
}