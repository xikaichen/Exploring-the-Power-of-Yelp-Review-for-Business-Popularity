#!/usr/bin/spark-submit

import sys
from pyspark import SparkContext
import json
import string
from string import translate




#split word return res
def split_len_word(line,length,remove_punctuation_map):
    line = line.translate(remove_punctuation_map).lower()
    word_list = line.split()
    res = []
    for i in xrange(0,len(word_list)-length+1):
        res.append(" ".join(word_list[i:i+length]))
    return res

# Find which words occurs most in high stars review
def find_best_words(review_data,n,length,cat):
    review_data = review_data.filter(lambda x: float(x['stars']) >= 4.0)
    review_text = review_data.map(lambda x: x['text'])
    punctuations='!"#$%&\'()*+,./:;<=>?@[\\]^_`{|}~'
    remove_punctuation_map = dict((ord(char), None) for char in punctuations)
    words = review_text.flatMap(lambda x:split_len_word(x,length,remove_punctuation_map))
    result = words.map(lambda x:(x,1)).reduceByKey(lambda x,y :x+y)
    result = result.map(lambda x:(x[1],x[0])).sortByKey(False).map(lambda x:(x[1],x[0]))
    total = total.collect()
    counts = result.take(n)
    with open(cat+"_good_words_"+str(length)+".txt",'w') as fout:
        for k,v in total:
            fout.write("{}\t{}\n".format(k.encode('utf-8'),v))
        for k,v in counts:
            fout.write("{}\t{}\n".format(k.encode('utf-8'),v))

def find_worst_words(review_data,n,length,cat):
    review_data = review_data.filter(lambda x: float(x['stars']) <= 2.0)
    review_text = review_data.map(lambda x: x['text'])
    punctuations='!"#$%&\'()*+,./:;<=>?@[\\]^_`{|}~'
    remove_punctuation_map = dict((ord(char), None) for char in punctuations)
    words = review_text.flatMap(lambda x:split_len_word(x,length,remove_punctuation_map))
    result = words.map(lambda x:(x,1)).reduceByKey(lambda x,y :x+y)
    result = result.map(lambda x:(x[1],x[0])).sortByKey(False).map(lambda x:(x[1],x[0]))
    counts = result.take(n)
    with open(cat+"_bad_words_"+str(length)+".txt",'w') as fout:
        for k,v in total:
            fout.write("{}\t{}\n".format(k.encode('utf-8'),v))
        for k,v in counts:
            fout.write("{}\t{}\n".format(k.encode('utf-8'),v))
            
# add categorites to review by joined business data
def add_cat(x):
    review = x[1][0]
    categories = x[1][1]
    review["categories"]=categories
    return review

def wordcount(business_data,review_data,n,length,cat):
    #if cat = "All" execute find_best_words() and find_worst_words()
    if cat != 'All':
        #join the review_data and business_data to get categorites
        business_data = business_data.map(lambda x:(x['business_id'],x['categories']))
        review_data = review_data.map(lambda x:(x['business_id'],x))
        review_data = review_data.join(business_data)
        rebiew_data = review_data.map(lambda x: add_cat(x))
        review_data = review_data.filter(lambda x: cat in x['categories'])
    find_best_words(review_data,n,length,cat)
    find_worst_words(review_data,n,length,cat)

def contruct_dictionary():
    good_words_file = open("",'r')
    bad_words_file = open("",'r')

def text_to_point(x,pos_dic,neg_dic,remove_punctuation_map):
    #remove all punctuation and then split the sentence
    line = x["text"].translate(remove_punctuation_map).lower()
    words = line.split()
    pos_words_num = 0
    neg_words_num = 0
    total_words_num = len(words)
    review_star = x['stars']
    #nots word
    nots = ['not','no','never','seldom','nothing','hardly']
    for i in xrange(0,len(words)):
        if words[i] in pos_dic:
            #positive words occurs and there is no nots words around it
            if (i >=3) and ((words[i-1] in nots) or (words[i-2] in nots) or (words[i-3] in nots) ):
                neg_words_num += 1
            else:
                pos_words_num += 1
        if words[i] in neg_dic:
            neg_words_num += 1
            #it will return star, positive words number, negtive number, total words number
    return (review_star,pos_words_num,neg_words_num,total_words_num)
    

def wordmap(review_data):
    '''
    generate two file positive-words.txt and negative-words.txt
    '''
    pos_dic_file =  'positive-words.txt'
    neg_dic_file =  'negative-words.txt'
    pos_dic = {}
    neg_dic = {}
    with open(pos_dic_file,'r') as fin:
        for line in fin:
            word = line.strip()
            pos_dic[word] = 1
        fin.close()
    with open(neg_dic_file,'r') as fin:
        for line in fin:
            word = line.strip()
            neg_dic[word] = 1
        fin.close()
    punctuations='!"#$%&\'()*+,./:;<=>?@[\\]^_`{|}~'
    remove_punctuation_map = dict((ord(char), None) for char in punctuations)
    data = review_data.map(lambda x:text_to_point(x,pos_dic,neg_dic,remove_punctuation_map))
    data = data.collect()
    with open("regression_review_text",'w') as fout:
        #write four things, star, positive words number, negative words number, total words number
        #into regression_review_text
        for v1,v2,v3,v4 in data:
            fout.write("{}\t{}\t{}\t{}\n".format(v1,v2,v3,v4))


if __name__ == "__main__":
    
    business_file =  '/Users/xikai_chen/BDA_Final_Project/text/yelp_academic_dataset_business.json'
    user_file =  '/Users/xikai_chen/BDA_Final_Project/text/yelp_academic_dataset_user.json'
    review_file =  '/Users/xikai_chen/BDA_Final_Project/text/yelp_academic_dataset_review.json'
    tip_file =  '/Users/xikai_chen/BDA_Final_Project/text/yelp_academic_dataset_tip.json'
    checkin_file =  '/Users/xikai_chen/BDA_Final_Project/text/yelp_academic_dataset_checkin.json'
    #initial a spark context object sc
    sc     = SparkContext( appName="Business_stats" )
    #load data
    business_data = sc.textFile(business_file).map(lambda x: json.loads(x))
    user_data = sc.textFile(user_file).map(lambda x: json.loads(x))
    review_data = sc.textFile(review_file).map(lambda x: json.loads(x))

    cat = "Restaurants"
    cat = "All"

    wordmap(review_data)


    
    sc.stop()
