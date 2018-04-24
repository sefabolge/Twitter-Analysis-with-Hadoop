import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.List;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Map extends Mapper<LongWritable, Text, Text, IntWritable> {


	List<String> athletesInfo = new ArrayList<String>();

	private final IntWritable one = new IntWritable(1);
	private Text data = new Text();


	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		String[] fields = value.toString().split(";");

		if (fields.length == 4) {
			for (int i = 0; i < athletesInfo.size(); i++) {
				if (fields[2].contains(athletesInfo.get(i))) {
					data.set(athletesInfo.get(i));
					context.write(data, one);
				}
			}
		}
	}

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {

		URI fileUri = context.getCacheFiles()[0];

		FileSystem fs = FileSystem.get(context.getConfiguration());
		FSDataInputStream in = fs.open(new Path(fileUri));

		BufferedReader br = new BufferedReader(new InputStreamReader(in));

		String line = null;

		try {

			br.readLine();

			while ((line = br.readLine()) != null) {

				String[] fields = line.split(",");
				if (fields.length == 11) {
					athletesInfo.add(fields[1]);
				}
			}
			br.close();

		} catch (Exception e) {

		}
		super.setup(context);
	}

}
