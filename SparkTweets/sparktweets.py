from pyspark.sql import SparkSession

spark = SparkSession\
        .builder\
        .appName("TweetStats")\
        .getOrCreate()
sc = spark.sparkContext

tweets = sc.textFile("hdfs:///user/group2/trump_tweets.csv")

def findDate(s):
	import re
	patt = re.compile(".*(\d\d-\d+-\d+ \d+:\d+:\d+).*")
	try:
		date = re.search(patt, s).groups(0)[0]
		return date
	except:
		return ""

dates = tweets.map(lambda tweet : findDate(tweet))

def yearHour(s):
	try:
		year = s.split(' ')[0].split('-')[2]
		hour = s.split(' ')[1].split(':')[0]
		return (int(year), hour)
	except:
		return (None, None)

dates = dates.map(lambda date : yearHour(date))
mapped = dates.map(lambda date : (date, 1))
combined = mapped.reduceByKey(lambda a,b : a+b)
combined = combined.map(lambda value : (value[0][0], (value[0][1], value[1])))

def joinlist(a, b):
	if type(a) is list and type(b) is list:
		return a + b
	elif type(a) is list and type(b) is not list:
		return a.append(b)
	else:
		return list(a).append(b)

hourPct = combined.map(lambda a : (a[0], list(a[1])))
hourPct = hourPct.reduceByKey(lambda a, b : joinlist(a, b) )

def sortlist(a):
	newlist = []
	from math import ceil
	for i in range(24):
		add = 0
		for j in range(ceil(len(a)/2)):
			if a[j*2] == str(i).zfill(2):
				newlist.append(a[j*2+1])
				add = 1
		if add == 0:
			newlist.append(0)
		del add
	return newlist

hourPct = hourPct.map(lambda a : (a[0], sortlist(a[1])))

def listavg(a):
	if sum(a) > 0:
		tot = sum(a)
		for i in range(len(a)):
			a[i] = str(int(a[i] / tot * 100)).zfill(2)
		a = (a, tot)
		return a
	else:
		return (0, 0)

hourPct = hourPct.map(lambda a : (a[0], listavg(a[1])))

def avg(year, tweets):
	if type(year) is not int:
		return 0
	elif year == 2009:
		return tweets/242
	elif year == 2019:
		return tweets/327
	elif year % 4 == 0:
		return tweets/366
	else:
		return tweets/365

hourPct = hourPct.map(lambda a : (a[0], a[1][0], a[1][1]))
hourPct = hourPct.map(lambda a : (a[0], a[1], round(avg(a[0], a[2]), 1)))

def tostr(a):
	out = str(a[0])
	for i in range(1, len(a)):
		if type(a[i]) is list:
			for j in range(len(a[i])):
				out += " " + str(a[i][j])
		else:
			out += " " + str(a[i])
	return out

output = hourPct.map(lambda a : tostr(a))
output.collect()
output.saveAsTextFile("hdfs:///user/group2/sparktweetsss")