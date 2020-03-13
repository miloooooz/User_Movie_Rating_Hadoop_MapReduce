# User_Movie_Rating_Hadoop_MapReduce
Big Data Management Project-User_Movie_Rating_Hadoop_MapReduce
## Introduction:
This program summarize the intput.txt which contains rows of **userId, movieId, rating, timestamp** and format them into the key value pairs where key is the movie pairs and value is the array of User-Rating triples.

Here are one example:

### Input format:
> N0,N1,4
> 
> N0,N2,3
>
> N1,N2,2
>
> N1,N3,2
>
> N2,N3,7
>
> N3,N4,2
>
> N4,N0,4
>
> N4,N1,4
>
> N4,N5,6

### Output format:
> N2,3,N0-N2
>
> N1,4,N0-N1
>
> N3,6,N0-N1-N3
>
> ...

## Instruction:
Download the ***AssigOnez5223541.java*** and ***Hadoop-Core.jar***.

cmd run `javac -cp ".:Hadoop-Core.jar" AssigOnez5223541.java`

cmd run `java -cp ".:Hadoop-Core.jar" AssigOnez5223541 input.txt output`

The output file will be contained in the directory ***output***, there will be two directories ***out1*** and ***out2***. The final result is contained in ***out2***.

Or you can directly download all the *.class* files representing all the classes contained in the file and then run `java -cp ".:Hadoop-Core.jar" AssigOnez5223541 input.txt output`
