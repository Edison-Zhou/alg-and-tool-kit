#!/usr/bin/env bash

# used for doraemonBiz standalone running
mainclass=cn.moretv.doraemon.biz.tabReorder.User2GroupByALS
current_bin_path="`dirname "$0"`"
cd ${current_bin_path}
sh ${current_bin_path}/../bin/submit.sh ${mainclass} wang.baozhi_${mainclass} --env pro \
        --videoType movie --numOfDays 20 --score 0.1  --userGroup mtu \
        --ifABtest false --alg reorderALS --offset 3000

