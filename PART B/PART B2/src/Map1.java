import java.io.IOException;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Map1 extends Mapper<LongWritable, Text, Text, IntWritable> {
	private final IntWritable one = new IntWritable(1);
	private Text data = new Text();

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		int myTime = 22;
		String[] fields = value.toString().split(";");

		if (fields.length == 4) {
			try {
				long epochTime = Long.parseLong(fields[0]);
				LocalDateTime d = LocalDateTime.ofEpochSecond(epochTime / 1000, 0, ZoneOffset.of("-3"));
				if (d.getHour() == myTime) {

					String tweets = fields[2];
					String[] hashtags = tweets.split(" ");

					for (String hashtag : hashtags) {
						String retVal = hashtag.replaceAll("[_|$<>\\^=\\[\\]\\*/\\,;,.\\-:()?!\\\"']", "");

						if (retVal.startsWith("#")) {

							data.set(retVal.toLowerCase());
							context.write(data, one);
						}
					}
				}

			} catch (Exception e) {
			}
		}
	}
}
