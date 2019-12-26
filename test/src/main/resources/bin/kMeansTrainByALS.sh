#!/usr/bin/env bash

# used for doraemonBiz standalone running
mainclass=cn.moretv.doraemon.biz.tabReorder.KMeansTrainByALS
current_bin_path="`dirname "$0"`"
cd ${current_bin_path}
sh ${current_bin_path}/../bin/submit.sh ${mainclass} wang.baozhi_${mainclass} --env dev \
        --videoType movie  --numIter 50 --kNum 700 \
        --numOfDays 7 --score 0.1

