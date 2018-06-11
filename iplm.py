#Spark UseCase Solution

from __future__ import print_function
from os.path import expanduser, join, abspath
from pyspark.sql import SparkSession,Row
from pyspark.sql.types import *
from pyspark.sql.readwriter import DataFrameWriter

if __name__ == "__main__":
    warehouse_location = abspath('spark-warehouse')
    spark = SparkSession.builder.appName("IPL Data Analysis").config("spark.sql.warehouse.dir", warehouse_location).enableHiveSupport().getOrCreate()

    spark.sql("create  table if not exists iplmat(id int,season int,city string,date string,team1 string,team2 string,toss_winner string,toss_decision string,result string,dl_applied int,winner string,win_by_runs int,win_by_wickets int,player_of_match string,venue string,umpire1 string,umpire2 string,umpire3 string) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' STORED AS TextFile") 
    spark.sql("create  table if not exists tempo(stadium string,bat_first int) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' STORED AS TextFile")
    spark.sql("create  table if not exists ipl1(venue string,total_matches int,batting_first_won int,win_percentage int) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' STORED AS TextFile")

    # reading the data set
    df = spark.read.csv("/home/tom/matches.csv")
    df.write.insertInto("iplmat",overwrite = True)
    df = spark.sql("SELECT * from iplmat")

    
    # player with the maximum number of player of match awarded
    a = df.select("player_of_match").groupBy("player_of_match").count().sort("count",ascending=False).limit(10)
    a.show()

    # best stadium for batting first
    b= df.select(("toss_decision"), ("win_by_runs"), ("venue")).filter(df.win_by_runs >0).groupBy("venue").count().sort("count",ascending=False)
    c= df.select(("venue")).groupBy("venue").count().sort("count",ascending=False)
    b.write.insertInto("temp",overwrite = True)
    t = spark.sql("SELECT * from tempo")
    p=c.join(t, c.venue == t.stadium, 'inner')
    k=p.selectExpr("venue","count","bat_first", "(bat_first/count)*100").sort("((bat_first / count) * 100)",ascending=False).limit(10)
    k.write.insertInto("ipl1",overwrite = True)
    x = spark.sql("SELECT * from batf")
    x.show()
    spark.stop()
