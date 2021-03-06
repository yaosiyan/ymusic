# coding:utf-8

userRatingDic = {}  # {user:{track:rating}}
# track_user_rating = {}
userAvg = {}  # {user:averageRating}
testDict = {}  # test_user_track_rating_Dict


def handler(trainfile, testfile):
    rating_sum = 0.0
    lastUsedid = -1
    for line in open(trainfile, 'r'):  # 打开指定文件
        (trackid, rating, usedid, ts) = line.strip().split('\t')  # 数据集中每行有4项
        trackid = int(trackid)
        usedid = int(usedid)
        rating = float(rating)
        userRatingDic.setdefault(usedid, {})  # 设置字典的默认格式,元素是user:{}字典
        userRatingDic[usedid][trackid] = rating
        if usedid == lastUsedid:          # [优化]如果已经出现过usedid,累计rating,
            rating_sum += rating  # if usedid in userAvg.keys():rating_sum += userAvg[usedid]
        else:
            # 若usedid改变
            userAvg[lastUsedid] = rating_sum  # 保存上一个usedid的评分总数
            lastUsedid = usedid  # 更新lastUsedid
            rating_sum = rating  # 初始化rating_sum
    del userAvg[-1]  # ???无语了,KeyError
    for userid in userAvg:
        userAvg[userid] /= float(len(userRatingDic[userid]))
        # print userid, userAvg[userid]

    for line in open(testfile, 'r'):  # 打开指定文件
        (trackid, rating, usedid, ts) = line.strip().split('\t')  # 数据集中每行有4项
        # 设置字典的默认格式,元素是user:{}字典
        trackid = int(trackid)
        usedid = int(usedid)
        testDict.setdefault(usedid, [])
        testDict[usedid].append(trackid)
    return userRatingDic, testDict, userAvg


if __name__ == '__main__':
    from pyspark import SparkContext

    sc = SparkContext("local[2]", "dataHandler APP")
    dataPath = '/Users/ibunny/Data/Webscope_C15/ydata-ymusic-kddcup-2011-track1/'
    user_item_rating_Dic, testDict, userAvg = handler(
            dataPath + 'mapped_trainIdx1.txt',
            dataPath + 'mapped_validationIdx1.txt')  # RDD
    try:
        import cPickle as pickle
    except ImportError:
        import pickle
    f1 = open('dumps_userRatingDic.txt', 'wb+')
    f2 = open('dumps_testDict.txt', 'wb+')
    f3 = open('dumps_userAvg.txt', 'wb+')

    pickle.dump(user_item_rating_Dic, f1)
    print 'f1 done!'
    pickle.dump(testDict, f2)
    print 'f2 done!'
    pickle.dump(userAvg, f3)
    print 'f3 done!'
    f1.close()
    f2.close()
    f3.close()
