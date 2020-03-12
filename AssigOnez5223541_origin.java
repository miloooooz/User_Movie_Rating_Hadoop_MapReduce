/*
Documentation:
In this project, there are two mapper functions and two reducer functions.
The first mapreduce aims to map the users as keys and the Writable group (movies, rate) as values.

UserMapper split the input strings into three parts: userid, movieid and rates. 
It takes UserId (Text) as key and MovieRating (MapWritable with movieId as key and ratings as value) as value

UserReducer then reduce the MovieRating together for the same user and return UserId as keys, MovieRating as values.

MovieMapper takes the output from UserReducer, and it extracts the MovieId from MovieRating MapWritable and combines
them into different pair combinations, the obtained pairs are regarded as keys for the MovieMapper function in the form of MoviePairWritable.
Then I defined a customised Writable type called UserRatingWritable. It contains UserId, Rate1 and Rate2 to form
the tuple to be values of MovieMapper. I overrode toString() function to make the UserRatingWritable to be shown as a tuple containing
UserId, Rate1, Rate2.
So finally MovieMapper takes UserId as keys, MovieRatings as values and returns MoviePairs as keys, UserRatings as values.

MovieReducer has inputs MoviePairs and UserRatings, reduce different UserRatings together for the same MoviePairs.
And then it return distinct MoviePairs as keys and the list of UserRatings tuples as values.

MovieReducer
*/
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Set;

public class AssigOnez5223541 {

    public static class MoviePairWritable implements WritableComparable{

        private Text movie1;
        private Text movie2;

        public MoviePairWritable(){
            this.movie1 = new Text("");
            this.movie2 = new Text("");
        }

        public Text getMovie1() {return movie1;}

        public void setMovie1(Text movie1) {this.movie1 = movie1;}

        public Text getMovie2() {return movie2;}

        public void setMovie2(Text movie2) {this.movie2 = movie2;}

        @Override
        public String toString() {
            return "(" + this.movie1.toString() + "," + this.movie2.toString() + ")";
        }

        @Override
        public boolean equals(Object obj) {
            if (obj instanceof MoviePairWritable){
                MoviePairWritable mp = (MoviePairWritable) obj;
                return movie1.equals(mp.movie1) && movie2.equals(mp.movie2);
            }
            return false;
        }

        @Override
        public int compareTo(Object o) {
            MoviePairWritable mp = (MoviePairWritable) o;
            int comp = movie1.compareTo(mp.movie1);
            if (comp != 0){
                return comp;
            }
            return movie2.compareTo(mp.movie2);
        }

        @Override
        public void write(DataOutput data) throws IOException {
            this.movie1.write(data);
            this.movie2.write(data);
        }

        @Override
        public void readFields(DataInput data) throws IOException {
            this.movie1.readFields(data);
            this.movie2.readFields(data);
        }
    }

    public static class UserRatingWritable implements Writable{
    	// This class defines a customised class which provides the tuple (userid, rate1 and rate2) for final result
        private Text userid;
        private IntWritable rate1;
        private IntWritable rate2;

        public UserRatingWritable(){
            this.userid = new Text("");
            this.rate1 = new IntWritable(-1);
            this.rate2 = new IntWritable(-1);
        }

        public Text getUserid() {return userid;}

        public void setUserid(Text userid) {this.userid = userid;}

        public IntWritable getRate1() {return rate1;}

        public void setRate1(IntWritable rate1) {this.rate1 = rate1;}

        public IntWritable getRate2() {return rate2;}

        public void setRate2(IntWritable rate2) {this.rate2 = rate2;}

        @Override
        public String toString() {
            return "(" + this.userid.toString() + "," + this.rate1.toString() + "," + this.rate2.toString() + ")";
        }

        @Override
        public void write(DataOutput data) throws IOException {
            this.userid.write(data);
            this.rate1.write(data);
            this.rate2.write(data);
        }

        @Override
        public void readFields(DataInput data) throws IOException {
            this.userid.readFields(data);
            this.rate1.readFields(data);
            this.rate2.readFields(data);
        }
    }


    public static class UserMapper extends Mapper<LongWritable, Text, Text, MapWritable>{
    	// This class allows us to split the input text into userid, movieid, rate1 and rate2 first.
    	// Then it stores movieid and the corresponding rate from each line into one MapWritable
    	// Finally the userid from each line and its corresponding movie-rate MapWritable will be mapped together

        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] parts = value.toString().split("::");  // split to the format of [User, Movie, Rating]
            MapWritable movieRating = new MapWritable();
            movieRating.put(new Text(parts[1]), new IntWritable(Integer.parseInt(parts[2])));    // here the key is movie, value is rating
            context.write(new Text(parts[0]), movieRating);
        }
    }

    public static class UserReducer extends Reducer<Text, MapWritable, Text, MapWritable>{
        protected void reduce(Text key, Iterable<MapWritable> values, Context context) throws IOException, InterruptedException{
            // Combine the MovieRating pairs for the same users
            MapWritable val = new MapWritable();
            for(MapWritable a:values)
                val.putAll(a);
            context.write(key, val);
            // finally return format: User movie1 rate1 movie2 rate2 .....
        }
    }

    public static class MovieMapper extends Mapper<Text, MapWritable, MoviePairWritable, UserRatingWritable>{
    	// This class reverse the position of userid and movieid. It finds the movie pairs that both movies has been watched
    	// by the same users. Then we use UserRatingWritable to map users and ratings together into one tuple.
        // The output for this class is:  (movie pairs) (UserId, Rating1, Rating2)
        @Override
        protected void map(Text key, MapWritable value, Context context) throws IOException, InterruptedException {
            Set<Writable> movies = value.keySet();
            ArrayList<Text> mov = new ArrayList<Text>();
            for (Writable m:movies)
                mov.add(new Text(m.toString()));

            for (int i = 0; i < mov.size()-1; i++) {
                for (int j = i + 1; j < mov.size(); j++) {
                    MoviePairWritable key_pairs = new MoviePairWritable();
                    UserRatingWritable value_groups = new UserRatingWritable();
                    value_groups.setUserid(new Text(key.toString()));
                    if (mov.get(j).toString().compareTo(mov.get(i).toString()) > 0) {
                        key_pairs.setMovie1(new Text(mov.get(i).toString()));
                        key_pairs.setMovie2(new Text(mov.get(j).toString()));
                        value_groups.setRate1(new IntWritable(Integer.parseInt(value.get(mov.get(i)).toString())));
                        value_groups.setRate2(new IntWritable(Integer.parseInt(value.get(mov.get(j)).toString())));
                    }
                    else {
                        key_pairs.setMovie1(new Text(mov.get(j).toString()));
                        key_pairs.setMovie2(new Text(mov.get(i).toString()));
                        value_groups.setRate1(new IntWritable(Integer.parseInt(value.get(mov.get(j)).toString())));
                        value_groups.setRate2(new IntWritable(Integer.parseInt(value.get(mov.get(i)).toString())));
                    }
                    context.write(key_pairs, value_groups);
                }
            }
        }
    }

    public static class MovieReducer extends Reducer<MoviePairWritable, UserRatingWritable, MoviePairWritable, Text>{
        @Override
        protected void reduce(MoviePairWritable movies, Iterable<UserRatingWritable> users_ratings, Context context) throws IOException, InterruptedException {
            // Combine the UserRating pairs for the same moviepairs
            String val = "";
            for (UserRatingWritable u:users_ratings)
                val += new Text(u.toString() + ",");

            context.write(movies, new Text("[" + val.substring(0, val.length()-1) + "]"));
            // finally return format: Moviepairs UserRating1 UserRating2 .....
        }
    }



    public static void main(String[] args) throws Exception {
        Path out = new Path(args[1]);
        Configuration conf = new Configuration();
        Job job1 = Job.getInstance(conf, "Find Users");

        job1.setMapperClass(UserMapper.class);
        job1.setReducerClass(UserReducer.class);

        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(MapWritable.class);
        job1.setOutputFormatClass(SequenceFileOutputFormat.class);

        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(out, "out1"));

        if(!job1.waitForCompletion(true))
            System.exit(1);

        Job job2 = Job.getInstance(conf, "Reduce Rates");

        job2.setMapperClass(MovieMapper.class);
        job2.setReducerClass(MovieReducer.class);

        job2.setOutputKeyClass(MoviePairWritable.class);
        job2.setOutputValueClass(Text.class);
        job2.setMapOutputKeyClass(MoviePairWritable.class);
        job2.setMapOutputValueClass(UserRatingWritable.class);

        job2.setInputFormatClass(SequenceFileInputFormat.class);
        FileInputFormat.addInputPath(job2, new Path(out, "out1"));
        FileOutputFormat.setOutputPath(job2, new Path(out, "out2"));

        if(!job2.waitForCompletion(true))
            System.exit(1);
    }
}
