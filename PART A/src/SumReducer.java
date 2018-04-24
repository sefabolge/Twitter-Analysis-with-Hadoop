import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class SumReducer extends Reducer<TwitterFields, IntWritable, Text, IntWritable> {

    private Text id = new Text();

    public void reduce(TwitterFields key, Iterable<IntWritable> values, Context context)

              throws IOException, InterruptedException {

    	 int sum = 0;
        for (IntWritable value : values) {

        	sum=sum + value.get();
        }

        // There is more than 140 character of tweet and we filtered it..
        if(key.getMultiplier() > 28) {
        	return;
        }

        id.set(Integer.toString(((key.getMultiplier()-1)*5)+1)+ "—" + Integer.toString(key.getMultiplier()*5));
        context.write(id, new IntWritable(sum));

    }

}
