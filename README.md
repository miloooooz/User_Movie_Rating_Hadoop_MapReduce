# User_Movie_Rating_Hadoop_MapReduce
Big Data Management Project-User_Movie_Rating_Hadoop_MapReduce
## Introduction:
This program summarize the intput.txt which contains rows of **userId, movieId, rating, timestamp** and format them into the key value pairs where key is the movie pairs and value is the array of User-Rating triples.

Here are one example:

### Input format:
> U1::M1::2::11111111
> 
> U2::M2::3::11111111
> 
> U2::M3::1::11111111
> 
> U3::M1::4::11111111
> 
> U4::M2::5::11111111
>
> U5::M2::3::11111111
>
> U5::M1::1::11111111
>
> U5::M3::3::11111111

### Output format:
> (M1,M2) [(U5,1,3)]
>
> (M2,M3) [(U5,3,3),(U2,3,1)]
>
> (M1,M3) [(U5,1,3)]


## Instruction:
Download the ***AssigOnez5223541.java*** and ***Hadoop-Core.jar***.

cmd run `javac -cp ".:Hadoop-Core.jar" AssigOnez5223541.java`

cmd run `java -cp ".:Hadoop-Core.jar" AssigOnez5223541 input.txt output`

The output file will be contained in the directory ***output***, there will be two directories ***out1*** and ***out2***. The final result is contained in ***out2***.

Or you can directly download all the *.class* files representing all the classes contained in the file and then run `java -cp ".:Hadoop-Core.jar" AssigOnez5223541 input.txt output`
