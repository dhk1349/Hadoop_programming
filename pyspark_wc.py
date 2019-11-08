from operator import add
sample=["cat","ele","rat","rat","cat"]

x=sc.parallelize(sample)
tmp=x.map(lambda x:(x,1)).collect()
rdd=sc.parallelize(tmp)

result=rdd.reduceByKey(add).collect()
print result
