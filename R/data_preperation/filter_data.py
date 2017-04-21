#!/usr/bin/spark-submit
#
# Choose data from raw data set
import sys
from pyspark import SparkContext
import json

# get the list of business that are top [N] [category] business from [city] with most reviews
def get_business_list(num,category,city,business_data):
    if city != "":
        cat_data = business_data.filter(lambda x: (category in x["categories"]) and (city in x["city"]))
    else:
        cat_data = business_data.filter(lambda x: (category in x["categories"]))
    bus = cat_data.map(lambda x: (x,x['review_count']))
    counts = bus.map(lambda x : (x[1],x[0])).sortByKey(False).map(lambda x: (x[1],x[0]))
    counts = counts.map(lambda x: (x[0]["business_id"],x[0]["name"],x[0]["stars"],x[1])).take(num)
    if city != "":
        city_name = "_".join(city.split())
    else:
        city_name = "All"
    file_name = "best_"+str(num)+"_"+category+"_in_"+city_name+".txt"
    with open(file_name,'w') as fout:
        for (bus,name,stars,count) in counts:
            fout.write("{}\t{}\t{}\t{}\n".format(bus.encode('utf-8'),name.encode('utf-8'),stars,count))
    return file_name

# filter and save data we need for training data set
def make_name(file_name,extra):
	a,b = file_name.split('.')
	name = a+'_'+extra+".json"
        return name
def get_data_according_to_list_file(bus_list_file,business_data,review_data,tip_data,user_data):
	#get business list according to list file
	handle = open(bus_list_file,'r')
        bus_list = []
        for l in handle:
            id_,name,start,count = l.split('\t')
            bus_list.append(id_)
	print bus_list
	#filter business 
	filtered_business_data = business_data.filter(lambda x:x["business_id"] in bus_list)
	outputFile = make_name(bus_list_file,"business")
	save_business = filtered_business_data.map(lambda x: json.dumps(x)).collect()
        with open(outputFile,'w') as fout:
            for record in save_business:
                fout.write(record+"\n")
        fout.close()
	#filter reviews
	filtered_review_data = review_data.filter(lambda x:x["business_id"] in bus_list)
	outputFile = make_name(bus_list_file,"reviews")
	save_review = filtered_review_data.map(lambda x: json.dumps(x)).collect()
        with open(outputFile,'w') as fout:
            for record in save_review:
                fout.write(record+"\n")
        fout.close()
	#filter tips
	filtered_tips_data = tip_data.filter(lambda x:x["business_id"] in bus_list)
	outputFile = make_name(bus_list_file,"tips")
	save_tip = filtered_tips_data.map(lambda x: json.dumps(x)).collect()
        with open(outputFile,'w') as fout:
            for record in save_tip:
                fout.write(record+"\n")
        fout.close()
	#filter users, get from review and tip data
	users_from_review = filtered_review_data.map(lambda x:x['user_id'])
	users_from_tip = filtered_tips_data.map(lambda x:x['user_id'])
	users = users_from_review.union(users_from_tip).distinct().collect()
	filtered_data = user_data.filter(lambda x:x["user_id"] in users)
	outputFile = make_name(bus_list_file,"users")
	save_user = filtered_data.map(lambda x: json.dumps(x)).collect()
        with open(outputFile,'w') as fout:
            for record in save_user:
                fout.write(record+"\n")
        fout.close()

# constrct dictionary for top [n] positive reviews/negative reviews
# Find which words occurs most in high stars review
def find_best_words(review_data,n):
    review_data = review_data.filter(lambda x: float(x['stars']) >= 4.0)
    review_text = review_data.map(lambda x: x['text'])
    words = review_text.flatMap(lambda x: x.split(" ")).map(lambda word: filter (unicode.isalpha,word))
    result = words.map(lambda x:(x,1)).reduceByKey(lambda x,y :x+y)
    result = result.map(lambda x:(x[1],x[0])).sortByKey(False).map(lambda x:(x[1],x[0]))
    counts = result.take(n)
    return counts
# Find which words occurs most in low stars review
def find_worst_words(review_data,n):
    review_data = review_data.filter(lambda x: float(x['stars']) <= 2.0)
    review_text = review_data.map(lambda x: x['text'])
    words = review_text.flatMap(lambda x: x.split(" ")).map(lambda word: filter (unicode.isalpha,word))
    result = words.map(lambda x:(x,1)).reduceByKey(lambda x,y :x+y)
    result = result.map(lambda x:(x[1],x[0])).sortByKey(False).map(lambda x:(x[1],x[0]))
    counts = result.take(n)
    return counts

def get_words(review_data,length,num_dic,num_sample):
	positive = find_best_words(length,review_data,num_sample)
	negetive = find_worst_words(length,review_data,num_sample)
	positive = sc.parallelize(positive).map(lambda x:x[0])
	negetive = sc.parallelize(negetive).map(lambda x:x[0])
	distinct_positive = positive.subtract(negetive)
	distinct_negetive = negetive.subtract(positive)
	distinct_positive = distinct_positive.take(num_dic)
	distinct_negetive = distinct_negetive.take(num_dic)
	with open("good_words.txt",'w') as fout:
            for word in distinct_positive:
                fout.write("{}\n".format(word.encode("utf-8")))
        with open("bad_words.txt",'w') as fout:
            for word in distinct_negetive:
                fout.write("{}\n".format(word.encode("utf-8")))


#construct training data and output csv file
#raw data format: for each resturant, display each review by date and user.
# 0_resturant_id 1_review_date 2_review_text 3_review_star 4_vote_1 5_vote_2 6_vote_3 7_user_id 8_elite 9_friend 10_fans


####################################################################################
#Here is the first model, use #of review each month to predict next month's #reviews
####################################################################################
#
def make_record(review,record):
	# tmp[#total,#5star,#4star,#3star,#2star,#1star]
	record[0] += 1
	star = int(review[2])
	if star == 5:record[1] += 1
	elif star == 4:record[2] += 1
	elif star == 3:record[3] += 1
	elif star == 2:record[4] += 1
	else:record[5] += 1
#design for combine last three month of record for a month's record
def combine_record(record_list):
	combined_record = [0,0,0,0,0,0]
	for record in record_list:
		for i in range(6):
			combined_record[i] += record[i]
	return combined_record

#give a rdd of (business,[review1,review2....reviewn])
#return (business,[(year_month1,data1),(year_month2,data2),....,(year_month3,data3)])
def group_review_by_time(x):
	#x :value of paired rdd [review1,review2....reviewn]
	#review : (date,text,stars,user_id)
	review_list = x[1]

	time_diction = {} #format: year_month:[#total,#5star,#4star,#3star,#2star,#1star]
	for review in review_list:
		if review[0] not in time_diction:
			time_diction[review[0]] = [0,0,0,0,0,0]
			make_record(review,time_diction[review[0]])
		else:
			make_record(review,time_diction[review[0]])
	time_diction = time_diction.items()
	time_data = [] #year_month,sum_of_last_three_month([#total,#5star,#4star,#3star,#2star,#1star])
	for i in xrange(0,len(time_diction)):
		#last_three = [[time_diction[i-1][1],[time_diction[i-2][1],[time_diction[i-3][1]]]]]
		record = (time_diction[i][0],time_diction[i][1])
		time_data.append(record)
        time_data = sorted(time_data)
        return time_data

# seems only need review dataset
def construct_training_data_v1(business_file,user_file,review_file,tip_file):
	#load filtered data
    #business_data = sc.textFile(business_file).map(lambda x: json.loads(x))
    #user_data = sc.textFile(user_file).map(lambda x: json.loads(x))
    review_data = sc.textFile(review_file).map(lambda x: json.loads(x))
    tip_data = sc.textFile(tip_file).map(lambda x: json.loads(x))
    #business = business_data.map(lambda x: (x['business_id']))
    review = review_data.map(lambda x:(x['business_id'],(x['date'][0:7],x['text'],x['stars'],x['user_id'])))
    data = review.groupByKey()
    data = data.map(lambda x :(x[0],group_review_by_time(x),len(x[1])))
    data = data.map(lambda x : (x[1][-1],x)).sortByKey(False).map(lambda x: (x[1][0],x[1][1]))
    data = data.flatMapValues(lambda x:x).collect()
    #data format : business_id,(year_month,[#total,#5star,#4star,#3star,#2star,#1star])

    #output file
    file_name = "training_data_v1.txt"
    with open(file_name,'w') as fout:
    	for k,v in data:
    		year_month,val = v
    		v0,v1,v2,v3,v4,v5 = val
    		fout.write("{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\n".format(k,year_month,v0,v1,v2,v3,v4,v5))
    		#business_id year_month sum(#review_last_3) sum(5_star_last_3) ... sum(1_star_last_3)



if __name__ == "__main__":
    

    business_file =  '/Users/xikai_chen/BDA_Final_Project/text/yelp_academic_dataset_business.json'
    user_file =  '/Users/xikai_chen/BDA_Final_Project/text/yelp_academic_dataset_user.json'
    review_file =  '/Users/xikai_chen/BDA_Final_Project/text/yelp_academic_dataset_review.json'
    tip_file =  '/Users/xikai_chen/BDA_Final_Project/text/yelp_academic_dataset_tip.json'
    checkin_file =  '/Users/xikai_chen/BDA_Final_Project/text/yelp_academic_dataset_checkin.json'

    sc     = SparkContext( appName="Business_stats" )

    business_data = sc.textFile(business_file).map(lambda x: json.loads(x))
    user_data = sc.textFile(user_file).map(lambda x: json.loads(x))
    review_data = sc.textFile(review_file).map(lambda x: json.loads(x))
    tip_data = sc.textFile(tip_file).map(lambda x: json.loads(x))

    # Procedures starts
    #fileanme = get_business_list(1000,"Restaurants","Las Vegas",business_data)
    #filename = "best_1000_Restaurants_in_All.txt"
    #get_data_according_to_list_file(filename,business_data,review_data,tip_data,user_data)
    review_sub_data = sc.textFile("best_1000_Restaurants_in_All_reviews.json").map(lambda x: json.loads(x))
    get_words(review_sub_data,1,500,5000)



    #Traning data for model v1
    business_file =  '/Users/xikai_chen/BDA_Final_Project/text/yelp_academic_dataset_business.json'
    user_file =  '/Users/xikai_chen/BDA_Final_Project/text/yelp_academic_dataset_user.json'
    review_file =  '/Users/xikai_chen/BDA_Final_Project/text/yelp_academic_dataset_review.json'
    tip_file =  '/Users/xikai_chen/BDA_Final_Project/text/yelp_academic_dataset_tip.json'
    checkin_file =  '/Users/xikai_chen/BDA_Final_Project/text/yelp_academic_dataset_checkin.json'
    
    #construct_training_data_v1(business_file,user_file,review_file,tip_file)

    # Procedures ends
    sc.stop()
