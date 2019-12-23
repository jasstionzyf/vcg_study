from vcgImageAI.comm.sparkBase import *
from vcgImageAI.comm.vcgUtils import *

sparkBase = SparkBase()
spark = sparkBase.createYarnSparkEnv()
sc = spark.sparkContext









def run():



    '''
    following is your own code
    '''

    def seq_op(accumulator, element):
        if (accumulator[1] > element[1]):
            return accumulator
        else:
            return element

    def comb_op(accumulator1, accumulator2):
        if (accumulator1[1] > accumulator2[1]):
            return accumulator1
        else:
            return accumulator2


    student_rdd = sc.parallelize([
        ("Joseph", "Maths", 83), ("Joseph", "Physics", 74), ("Joseph", "Chemistry", 91), ("Joseph", "Biology", 82),
        ("Jimmy", "Maths", 69), ("Jimmy", "Physics", 62), ("Jimmy", "Chemistry", 97), ("Jimmy", "Biology", 80),
        ("Tina", "Maths", 78), ("Tina", "Physics", 73), ("Tina", "Chemistry", 68), ("Tina", "Biology", 87),
        ("Thomas", "Maths", 87), ("Thomas", "Physics", 93), ("Thomas", "Chemistry", 91), ("Thomas", "Biology", 74),
        ("Cory", "Maths", 56), ("Cory", "Physics", 65), ("Cory", "Chemistry", 71), ("Cory", "Biology", 68),
        ("Jackeline", "Maths", 86), ("Jackeline", "Physics", 62), ("Jackeline", "Chemistry", 75),
        ("Jackeline", "Biology", 83),
        ("Juan", "Maths", 63), ("Juan", "Physics", 69), ("Juan", "Chemistry", 64), ("Juan", "Biology", 60)], 3)

    zero_val = ('', 0)
    aggr_rdd = student_rdd.map(lambda t: (t[0], (t[1], t[2]))).aggregateByKey(zero_val, seq_op, comb_op)

    aggr_rdd.toDF().show(10, False)
if __name__ == '__main__':
    run()