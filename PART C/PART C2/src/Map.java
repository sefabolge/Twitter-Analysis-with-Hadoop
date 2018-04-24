import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Map extends Mapper<LongWritable, Text, Text, IntWritable> {

	private Hashtable<String, String> athletesInfo;
	private final IntWritable one = new IntWritable(1);
	private Text data = new Text();

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		String[] fields = value.toString().split(";");

		if (fields.length == 4) {
			Set<String> keys = athletesInfo.keySet();
			for (String ky : keys) {
				if (fields[2].contains(ky)) {

					data.set(athletesInfo.get(ky));
					context.write(data, one);
				}
			}
		}
	}

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {

		athletesInfo = new Hashtable<String, String>();

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
					athletesInfo.put(fields[1], fields[7]);
				}

			}
			br.close();

		} catch (Exception e) {

		}
		super.setup(context);
	}

}
