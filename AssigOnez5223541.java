/*
Documentation:
In this project, there are two mapper functions and two reducer functions.
The first mapreduce aims to map the users as keys and the Writable group (movies, rate) as values.

UserMapper split the input strings into three parts: userid, movieid and rates. 
It takes UserId (Text) as key and MovieRating (Custom Writable with movieId as key and ratings as value) as value

UserReducer then reduce the MovieRating together for the same user and return UserId as keys, MovieRating as values.

MovieMapper takes the output from UserReducer, and it extracts the MovieId from MovieRating custom writable and combines
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
import org.hsqldb.User;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Set;

public class AssigOnez5223541 {

    public static class MovieRatingWritable implements Writable {

        private Text movie;
        private IntWritable rate;

        public MovieRatingWritable(){
            this.movie = new Text("");
            this.rate = new IntWritable(-1);
        }
        public MovieRatingWritable(Text a, IntWritable b){
            this.movie = a;
            this.rate = b;
        }

        public Text getMovie() {return movie;}

        public void setMovie(Text movie) {this.movie = movie;}

        public IntWritable getRate() {return rate;}

        public void setRate(IntWritable rate) {this.rate = rate;}

        @Override
        public void write(DataOutput data) throws IOException {
            this.movie.write(data);
            this.rate.write(data);
        }

        @Override
        public void readFields(DataInput data) throws IOException {
            this.movie.readFields(data);
            this.rate.readFields(data);
        }
    }

    public static class MovieRatingArray extends ArrayWritable{

        public MovieRatingArray() {
            super(MovieRatingWritable.class);
        }

        public MovieRatingArray(ArrayList<MovieRatingWritable> array) {
            super(MovieRatingWritable.class);
            MovieRatingWritable[] movie_rating_list = new MovieRatingWritable[array.size()];
            for(int i = 0; i < array.size(); i++){
                movie_rating_list[i] = new MovieRatingWritable(
                        new Text(array.get(i).getMovie()),
                        new IntWritable(array.get(i).getRate().get()));
            }
            set(movie_rating_list);
        }
    }

    public static class FinalArray extends ArrayWritable {

        public FinalArray(){
            super(UserRatingWritable.class);
        }

        public FinalArray(ArrayList<UserRatingWritable> array) {
            super(UserRatingWritable.class);
            UserRatingWritable[] user_rating_list = new UserRatingWritable[array.size()];
            for (int i = 0; i < array.size(); i++) {
                user_rating_list[i] = new UserRatingWritable(new Text(array.get(i).getUserid()),
                        new IntWritable(array.get(i).getRate1().get()),
                        new IntWritable(array.get(i).getRate2().get()));
            }
            set(user_rating_list);

        }

        @Override
        public UserRatingWritable[] get() {
            return (UserRatingWritable[]) super.get();
        }

        @Override
        public String toString() {
            UserRatingWritable[] ur = get();
            String ret = "";
            for (UserRatingWritable userRatingWritable : ur) {
                ret += userRatingWritable.toString() + ",";
            }
            return "[" + ret.substring(0,ret.length()-1) + "]";
        }

        @Override
        public void write(DataOutput out) throws IOException {
            super.write(out);
        }
    }

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

        public UserRatingWritable(Text text, IntWritable intWritable, IntWritable intWritable1) {
            this.userid = text;
            this.rate1 = intWritable;
            this.rate2 = intWritable1;
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


    public static class UserMapper extends Mapper<LongWritable, Text, Text, MovieRatingWritable>{
    	// This class allows us to split the input text into userid, movieid, rate1 and rate2 first.
    	// Then it stores movieid and the corresponding rate from each line into one ArrayWritable
    	// Finally the userid from each line and its corresponding movie-rate ArrayWritable will be mapped together

        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] parts = value.toString().split("::");  // split to the format of [User, Movie, Rating]
            MovieRatingWritable movieRating = new MovieRatingWritable(
                // MovieRatingWritable: (movie, rate)
                    new Text(parts[1]), new IntWritable(Integer.parseInt(parts[2])));// here the key is movie, value is rating
            context.write(new Text(parts[0]), movieRating);
        }
    }

    public static class UserReducer extends Reducer<Text, MovieRatingWritable, Text, MovieRatingArray>{
        protected void reduce(Text key, Iterable<MovieRatingWritable> values, Context context) throws IOException, InterruptedException{
            // Combine the MovieRating pairs for the same users
            ArrayList<MovieRatingWritable> movie_rating = new ArrayList<>();
            for(MovieRatingWritable mr:values){
                movie_rating.add(new MovieRatingWritable(new Text(mr.getMovie()), new IntWritable(mr.getRate().get())));
            }
            MovieRatingArray val = new MovieRatingArray(movie_rating);
            context.write(key, val);
            // finally return format: User movie1 rate1 movie2 rate2 .....
        }
    }

    public static class MovieMapper extends Mapper<Text, MovieRatingArray, MoviePairWritable, UserRatingWritable>{
        // It takes (User, MovieRatingPairs) as inputs.
    	// This class reverse the position of userid and movieid. It finds the movie pairs that both movies has been watched
    	// by the same users. Then we use UserRatingWritable to map users and ratings together into one tuple.
        // The output for this class is:  (movie pairs) (UserId, Rating1, Rating2)
        @Override
        protected void map(Text key, MovieRatingArray value, Context context) throws IOException, InterruptedException {
            ArrayList<MovieRatingWritable> movierate = new ArrayList<MovieRatingWritable>();
            // MovieRatingWritable: (movie, rate)
            // MovieRatingArray: [MovieRatingWritable]
            // UserRatingWritable: (UserId, MovieRating1, MovieRating2)
            for (Writable val : value.get()) {
                MovieRatingWritable mv = (MovieRatingWritable) val;
                movierate.add(new MovieRatingWritable(new Text(mv.getMovie()), new IntWritable(mv.getRate().get())));
            }

            for (int i = 0; i < movierate.size()-1; i++) {
                for (int j = i + 1; j < movierate.size(); j++) {
                    MoviePairWritable key_pairs = new MoviePairWritable();
                    UserRatingWritable value_groups = new UserRatingWritable();
                    value_groups.setUserid(new Text(key.toString()));
                    if (movierate.get(j).getMovie().compareTo(movierate.get(i).getMovie()) > 0) {
                        key_pairs.setMovie1(new Text(movierate.get(i).getMovie()));
                        key_pairs.setMovie2(new Text(movierate.get(j).getMovie()));
                        value_groups.setRate1(new IntWritable(movierate.get(i).getRate().get()));
                        value_groups.setRate2(new IntWritable(movierate.get(j).getRate().get()));
                    }
                    else {
                        key_pairs.setMovie1(new Text(movierate.get(j).getMovie()));
                        key_pairs.setMovie2(new Text(movierate.get(i).getMovie()));
                        value_groups.setRate1(new IntWritable(movierate.get(j).getRate().get()));
                        value_groups.setRate2(new IntWritable(movierate.get(i).getRate().get()));
                    }
                    context.write(key_pairs, value_groups);
                }
            }
        }
    }

    public static class MovieReducer extends Reducer<MoviePairWritable, UserRatingWritable, MoviePairWritable, FinalArray>{
        @Override
        protected void reduce(MoviePairWritable movies, Iterable<UserRatingWritable> users_ratings, Context context) throws IOException, InterruptedException {
            // Combine the UserRating pairs for the same moviepairs
            ArrayList<UserRatingWritable> user_rating_array = new ArrayList<>();
            for (UserRatingWritable a:users_ratings){
                user_rating_array.add(new UserRatingWritable(new Text(a.getUserid()),
                        new IntWritable(a.getRate1().get()),
                        new IntWritable(a.getRate2().get())));
            }
            FinalArray val = new FinalArray(user_rating_array);
            context.write(movies, val);
            // finally return format: Moviepairs UserRating1 UserRating2 .....
        }
    }



    public static void main(String[] args) throws Exception {
        Path out = new Path(args[1]);
        Configuration conf = new Configuration();
        Job job1 = Job.getInstance(conf, "Find Users");

        job1.setMapperClass(UserMapper.class);
        job1.setReducerClass(UserReducer.class);

        job1.setOutputKeyClass(Text.class); // The key for userReducer is userid
        job1.setOutputValueClass(MovieRatingArray.class);   // The value for userReducer is an array of movie-rating pairs
        job1.setMapOutputKeyClass(Text.class);  // Key for the UserMapper is userid
        job1.setMapOutputValueClass(MovieRatingWritable.class);     // Value for UserMapper is a movie-rating pair
        job1.setOutputFormatClass(SequenceFileOutputFormat.class);

        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(out, "out1"));

        if(!job1.waitForCompletion(true))
            System.exit(1);

        Job job2 = Job.getInstance(conf, "Reduce Rates");

        job2.setMapperClass(MovieMapper.class);
        job2.setReducerClass(MovieReducer.class);

        job2.setOutputKeyClass(MoviePairWritable.class);
        job2.setOutputValueClass(FinalArray.class);
        job2.setMapOutputKeyClass(MoviePairWritable.class);
        job2.setMapOutputValueClass(UserRatingWritable.class);

        job2.setInputFormatClass(SequenceFileInputFormat.class);
        FileInputFormat.addInputPath(job2, new Path(out, "out1"));
        FileOutputFormat.setOutputPath(job2, new Path(out, "out2"));

        if(!job2.waitForCompletion(true))
            System.exit(1);
    }
}
