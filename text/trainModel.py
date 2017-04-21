#!/usr/bin/spark-submit

from pyspark import SparkContext
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.feature import HashingTF
from pyspark.mllib.tree import RandomForest, RandomForestModel
from pyspark.mllib.util import MLUtils
from pyspark.mllib.tree import GradientBoostedTrees, GradientBoostedTreesModel
from pyspark.mllib.classification import SVMWithSGD, SVMModel

def ramdonForest(sc):
    data = MLUtils.loadLibSVMFile(sc, '/Users/xikai_chen/BDA_Final_Project/text/format_bin_regression_review_text_v3)

    (trainingData, testData) = data.randomSplit([0.7, 0.3])

    # Train a RandomForest model.
    model = RandomForest.trainClassifier(trainingData, numClasses=6, categoricalFeaturesInfo={},
                                         numTrees=3, featureSubsetStrategy="auto",
                                         impurity='gini', maxDepth=4, maxBins=32)


    predictions = model.predict(testData.map(lambda x: x.features))
    labelsAndPredictions = testData.map(lambda lp: lp.label).zip(predictions)
    testErr = labelsAndPredictions.filter(lambda (v, p): v != p).count() / float(testData.count())
    print('Test Error = ' + str(testErr))
    print('Learned classification forest model:')
    print(model.toDebugString())


    model.save(sc, "target/tmp/myRandomForestClassificationModel")
    sameModel = RandomForestModel.load(sc, "target/tmp/myRandomForestClassificationModel")

def Boosting(sc):
    data = MLUtils.loadLibSVMFile(sc, '/Users/xikai_chen/BDA_Final_Project/text/bin_regression_review_text_v3_v4')
    (trainingData, testData) = data.randomSplit([0.7, 0.3])

    model = GradientBoostedTrees.trainClassifier(trainingData,learningRate = 0.05,maxDepth=5,maxBins=32,categoricalFeaturesInfo={}, numIterations=300)

    predictions = model.predict(testData.map(lambda x: x.features))
    labelsAndPredictions = testData.map(lambda lp: lp.label).zip(predictions)
    haha = labelsAndPredictions.collect()
    with open('predictions.txt','w') as fout:
        for v,p in haha:
            fout.write(str(v)+" "+str(p)+'\n')
    testErr = labelsAndPredictions.filter(lambda (v, p): v != p).count() / float(testData.count())
    print('Test Error = ' + str(testErr))
    print('Learned classification GBT model:')
    print(model.toDebugString())

def parsePoint(line):
    values = [float(x) for x in line.split(' ')]
    return LabeledPoint(values[0], values[1:])



def main():
    sc     = SparkContext( appName="Train model" )

    Boosting(sc)

if __name__=="__main__":
    main()
