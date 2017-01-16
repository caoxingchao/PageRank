# encoding: utf-8

from __future__ import print_function

import sys
try:
    reload(sys)
    sys.setdefaultencoding('utf-8')
except Exception as e:
    pass

from pyspark.sql import SparkSession

def aggX(a, b):
    if type(a) == list:
        a.append(b)
        return a
    else:
        res = []
        res.append(a)
        res.append(b)
        return res


def main():
    spark = SparkSession\
        .builder\
        .appName("PageRank")\
        .getOrCreate()

    sc = spark.sparkContext
    rdd_records = sc.textFile(sys.argv[1]).map(lambda x: (x.replace('"', '').split())).cache()
    pages = rdd_records.flatMap(lambda x: [(x[0], 1), (x[1], 1)]).groupByKey().toDF(['page', 'useless']).drop('useless')#.persist()
    df_records = rdd_records.toDF(['page', 'link']).persist()
    df_records.createOrReplaceTempView('records')
    links = spark.sql("SELECT page, collect_set(link) AS links FROM records GROUP BY page")
    ranks_rdd = pages.rdd.map(lambda x: (x[0], 1.0))
    links_rdd = links.rdd

    def ff(x):
        page = x[0]
        links = x[1][1]
        rank = x[1][0]
        result = []
        if isinstance(links, list) and len(links) > 0:
            for page in links:
                result.append((page, rank*1.0/len(links)))
        else:
            result.append((page, rank))
        return result

    for i in range(100):
        ranks_rdd = ranks_rdd.leftOuterJoin(links_rdd).flatMap(ff).reduceByKey(lambda x,y: x+y).mapValues(lambda v: 0.15+0.85*v)

    ranks = ranks_rdd.toDF(['page', 'rank'])
    res = ranks.repartition(1).orderBy(ranks.rank.desc())
    res.write.csv(sys.argv[2])

    spark.stop()


if __name__ == "__main__":
    main()
