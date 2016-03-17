# coding:utf-8

"""
 [1]调整threshold,http://my.oschina.net/lionets/blog/284479?fromerr=3dOWxguB
 [2]调整KNN的K

"""
import logging
from math import sqrt
import time
#from dataHandler import handler
from pyspark import SparkContext
try:
    import cPickle as pickle
except ImportError:
    import pickle

sc = SparkContext("local[2]", "userCF")
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                    datefmt='%a, %d %b %Y %H:%M:%S',
                    filemode='w',
                    filename='UserCF.log')

console = logging.StreamHandler()
console.setLevel(logging.INFO)
formatter = logging.Formatter('%(name)-12s: %(levelname)-8s %(message)s')
console.setFormatter(formatter)
logging.getLogger('').addHandler(console)

# item_user_rating_Dic = {}  # 嵌套字典
user_item_rating_Dic = {}
itemDists = {}  # item之间的相似度
userAvg = {}  # user打分均值
userKNNDic = {}
halfSimDict = {}
userInfo = {}


def pearson_corr(user1, user2, user_item_rating_Dic, threshold):
    """
    :param user1: usedid1(int)
    :param user2: usedid2(int)
    :param user_item_rating_Dic: {usedid:{track1:90,track2:0},usedid2:{}}
    :param threshold: 阈值,两个用户至少xx首歌共同评分才计算相似度
    :return: pearson相似度

    """
    """
        [注意!]相似度矩阵只需要计算一半!!!
    """
    try:
        sim = halfSimDict[user2][user1]
    except KeyError:
        try:
            sim = halfSimDict[user1][user2]
        except KeyError:
            pass
        else:
            logging.info(str(user2)+' '+str(user1)+'found sim:'+str(sim))
            return sim
    else:
        logging.info(str(user2)+' '+str(user1)+'found sim:'+str(sim))
        return sim

    # To get both rated items
    both_rated = []
    for item in user_item_rating_Dic[user1].keys():
        if item in user_item_rating_Dic[user2].keys():
            both_rated.append(item)
    number_of_ratings = len(both_rated)

    # Checking for number of ratings in common 如果共同评分数小于阈值,不考虑
    if number_of_ratings == 0 or number_of_ratings < threshold:
        return 0

    # Add up all the preferences of each user
    user1_preferences_sum = sum(
            [user_item_rating_Dic[user1][item] for item in both_rated])
    user2_preferences_sum = sum(
            [user_item_rating_Dic[user2][item] for item in both_rated])

    # Sum up the squares of preferences of each user
    user1_square_preferences_sum = sum(
            [pow(user_item_rating_Dic[user1][item], 2) for item in both_rated])
    user2_square_preferences_sum = sum(
            [pow(user_item_rating_Dic[user2][item], 2) for item in both_rated])

    # Sum up the product value of both preferences for each item
    product_sum_of_both_users = sum([user_item_rating_Dic[user1][
                                         item] * user_item_rating_Dic[user2][item] for item in both_rated])

    # Calculate the pearson score
    numerator_value = product_sum_of_both_users - \
                      (user1_preferences_sum * user2_preferences_sum / number_of_ratings)
    denominator_value = sqrt((user1_square_preferences_sum - pow(user1_preferences_sum, 2) / number_of_ratings)
                             * (user2_square_preferences_sum - pow(user2_preferences_sum, 2) / number_of_ratings))
    if denominator_value == 0:
        try:
            halfSimDict[user1][user2] = 0
        except KeyError:
            halfSimDict.setdefault(user1,{})
            halfSimDict[user1][user2] = 0
        return 0
    else:
        r = numerator_value / denominator_value
        try:
            halfSimDict[user1][user2] = r
        except KeyError:
            halfSimDict.setdefault(user1,{})
            halfSimDict[user1][user2] = r
        if r==1.0:
            logging.info('r==1!!!!!!!'+str(user1)+' '+str(user2)+':'+str(user_item_rating_Dic[user1]))
        return r


def most_similar_users(user, user_item_rating_Dic, userInfoDict, K=100):
    """
    :param user:usedid
    :param user_item_rating_Dic:{usedid:{track1:90,track2:0},usedid2:{}}
    :param userInfoDict:{usedid:trackRatingCount} 每个用户的评分数目
    :param K: K近邻 (默认为100,后续进行参数调整/绘图)
    :return:(sim,otherUserid) 元组
    """
    '''
        考虑到实际数据中有很多pearsonr = 1.0
        只需要保存K名 pearsonr = 1.0的usedid
        [注意!]相似的用户只需要计算一遍就可以了!!!保存KNN!!!!!
    '''
    if userKNNDic.has_key(user):
        print 'userKNN found!',user
        return userKNNDic[user]

    scores = []
    tempList = []  # 存放r>0.6的相似度
    cnt1 = 0  # 1.0出现次数
    coverThreshold = 0
    for other_user in user_item_rating_Dic:
        if other_user == user:continue
        ratingCount = userInfoDict[user]  # 用户的评分数
        coverThreshold = int(ratingCount * 0.3)  # 至少评分歌曲数的1/3有重叠才计算pearsonr
        r = pearson_corr(user, other_user, user_item_rating_Dic, coverThreshold)  # threshold,调整参数
        if r == 1.0:  # 或者r>0.8??
            scores.append((r, other_user))
            cnt1 += 1
        elif r >= 0.5:
            tempList.append((r, other_user))
    if cnt1 < K:
        tempList.sort(reverse=True)
        if len(tempList)+cnt1 < K:  # 如果
            logging.info(str(user)+'tempList not enough:',str(tempList))
            scores.extend(tempList)
        else:
            scores.extend(tempList[:K - len(scores)])

    # scores = [(pearson_corr(user, other_user, user_item_rating_Dic, 100), other_user)
    #           for other_user in user_item_rating_Dic if (other_user != user)]
    # Sort the similar users so that highest scores user will appear at the
    # first
    # scores.sort(reverse=True)  # 考虑大部分scores都为1!!!!抽样???
    # if len(scores) <= K:  # 如果小于k，只选择这些做推荐。
    #     return scores
    # else:  # 如果>k,截取k个用户，注意返回的是元组(pearsonr,usedid)
    #     return scores[0:K],
    userKNNDic[user] = scores
    logging.info(str(user)+' knn:'+str(scores))
    return scores


def recommendation(usedid, trackid, userAvg, user_track_rating, userInfoDict):
    """
    :param usedid:  用户id(int)
    :param trackid: 歌曲id(int)
    :param userAvg: 用户评分均值(dict) {usedid:13.3333}
    :param user_track_rating: {usedid:{track1:90,track2:0},usedid2:{}}
    :param userInfoDict:{usedid:trackRatingCount} 每个用户的评分数目
    :return: rating(float) 预测评分值
    """
    """
        1:调整KNN中的K值
        2:

    """
    simSum = 0.0
    weightedAverage = 0.0
    KNN = most_similar_users(usedid, user_track_rating, userInfoDict, 300)
    averageOfUser = userAvg[usedid]
    for sim, another in KNN:
        averageOfAnother = userAvg[another]  # 另一个用户的打分均值
        simSum += abs(sim)  # 分母
        # try if trackid in user_track_rating[another]:
        try:
            tempRating = user_track_rating[another][trackid]
        except KeyError:  # trackid not in user_track_rating[another]
            continue
        else:
            weightedAverage += (tempRating -
                                averageOfAnother) * sim  # 累加，一些值为负
    # simSum为0，即该项目尚未被其他用户评分，这里的处理方法：返回用户平均分
    if simSum == 0:
        return averageOfUser
    else:
        return (averageOfUser + weightedAverage / simSum)


def main(testingDict, userAvg, user_track_rating, userInfoDict):
    outfile = open('result.txt', 'w+')
    for usedid in testingDict:  # test集合中每个用户
        for track in testingDict[usedid]:  # 对于test集合中每一个项目用base数据集,CF预测评分
            start = time.clock()
            rating = recommendation(usedid, track, userAvg,
                                    user_track_rating, userInfoDict)  # 基于训练集预测用户评分(用户数目<=K)
            outfile.write('%s\t%s\t%s\n' % (usedid, rating, track))
            logging.info(str(usedid)+'\t'+str(track)+'\t'+str(rating)+'\t'+'predictTime: '+str(time.clock() - start))
        outfile.flush()
    outfile.close()


if __name__ == '__main__':
    # dataPath = '/Users/ibunny/Data/Webscope_C15/ydata-ymusic-kddcup-2011-track1/'
    # start = time.clock()
    # user_item_rating_Dic, testDict, userAvg = handler(
    #         dataPath + 'mapped_trainIdx1.txt', dataPath + 'mapped_validationIdx1.txt')  # RDD
    # logging.info('load data costs: '+str(time.clock()-start))
    logging.info('start!')
    dataPath = '/Users/ibunny/Code/pySpace/PycharmProjects/ymusic/'
    f1 = open(dataPath + 'dumps_userRatingDic.txt', 'rb+')
    f2 = open(dataPath + 'dumps_testDict.txt', 'rb+')
    f3 = open(dataPath + 'dumps_userAvg.txt', 'rb+')
    f4 = open(dataPath + 'userInfo.txt', 'r+')
    start = time.clock()
    userItemRatingDic = pickle.load(f1)
    logging.info('userRating done! costs: '+str(time.clock() - start))
    testDict = pickle.load(f2)
    logging.info('testDict done!')
    userAvg = pickle.load(f3)
    logging.info('userAvg done!')
    for key in userAvg.keys()[:100]:
        print key, userAvg[key]
    for line in f4.readlines():
        [usedid, userRatingCount] = line.strip().split('|')
        userInfo[int(usedid)] = int(userRatingCount)
    print userInfo[0]
    f1.close()
    f2.close()
    f3.close()
    f4.close()
    start = time.clock()
    keys = userItemRatingDic.keys()[:100000]  # 取1w用户测试
    test_userItemRatingDic = {}
    for key in keys:
        test_userItemRatingDic[key] = userItemRatingDic[key]
    logging.info('len(keys) = '+str(len(test_userItemRatingDic)))
    logging.info('ratingDic[0]:'+str(test_userItemRatingDic[0]))
    logging.info('testDic[0]:'+str(testDict[0]))
    logging.info('userAvg[0]:'+str(userAvg[0])+'\n')
    main(testDict, userAvg, test_userItemRatingDic, userInfo)
