import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Map extends Mapper<LongWritable, Text, TwitterFields, IntWritable> {
    private final IntWritable one = new IntWritable(1);
    private TwitterFields data = new TwitterFields();

    private LongWritable epoch_time = new LongWritable();
    private LongWritable tweetId = new LongWritable();
    private Text tweet = new Text();
    private Text device = new Text();


    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

    	//splitting the dataset with semicolon
    	String [] fields = value.toString().split(";");

    	if(fields.length == 4) {
    		try {

    			epoch_time.set(Long.parseLong(fields[0].trim()));
    			tweetId.set(Long.parseLong(fields[1].trim()));
    			tweet.set(fields[2]);
    			device.set(fields[3]);

    			data.set(epoch_time, tweetId, tweet, device);

			} catch (Exception e) {

			}
    		context.write(data,one);
    	}

    }
}
