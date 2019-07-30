--First need to create directory in HDFS.Creating a directory called movies_data_analysis

--bin/hadoop fs -mkdir /movies_data_analysis

--Putting DataFile(movies.txt) into HDFS by Using below command

--bin/hadoop fs -put /home/bhushan/Movies_Data_Analysis_using_Apache_pig/movies.txt /movies_data_analysis

--(Now, Execute each usecase by removing comment)



movies = load '/movies_data_analysis/movies.txt' USING PigStorage(',') as (id:int,name:chararray,year:int,rating:double,duration:int);


-- 1) find the number of movies released between 1950 and 1960


--filter_by_year = filter movies by year >= 1950 and year <= 1960;
--group_by_al = group filter_by_year all;
--for_each = foreach group_by_al generate COUNT(filter_by_year.id);
--dump for_each;
--for_eachh = foreach group_by_al generate $01, $02;
--dump for_eachh;

--Answer = including year 1950/1960 are 547 and excluding year 1950/1960 are 414... 




-- 2) Find the number of movies with duration more than 2 hours (7200 second). 


--filter_by_duration = filter movies by duration > 7200;
--group_by_al = group filter_by_duration all;
--for_each = foreach group_by_al generate COUNT(filter_by_duration);
--dump for_each;

-- Answer = 641 movies out there with duration more than 2 hours



-- 3) Find the list of years and number of movies released each year 

 
--B = group movies by year;
--C = foreach B generate group, COUNT(movies.year);
--dump C;



-- 4) Find the number of movies having rating more than 4.


--filter_by_rating = filter movies by rating > 4;
--group_by_al = group filter_by_rating all;
--for_each = foreach group_by_al generate COUNT(filter_by_rating);
--dump for_each;
--for_eachh = foreach group_by_al generate $01;
--dump for_eachh;

-- Answer = 897 movies having the rating more than 4..



-- 5) Find the movies whose rating are between 3 and 4


--filter_by_rating = filter movies by rating > 3 and rating < 4;
--group_by_al = group filter_by_rating all;
--for_each = foreach group_by_al generate COUNT(filter_by_rating); -- to count the movies
--dump for_each;
--for_eachh = foreach group_by_al generate $01; -- list of movies with names released between rating 3 and 4
--dump for_eachh;

--Answer = 7161 movies out there whose rated between 3 and 4..



-- 6) Find the total number of movies in the dataset
 

--group_movies = group movies all;
--for_each = foreach group_movies generate COUNT(movies);
--dump for_each;

-- Answer = 49590 movies are present in dataset

