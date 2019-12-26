#!/usr/bin/env bash

# used for doraemonBiz standalone running
mainclass=cn.moretv.doraemon.biz.tabReorder.TreeSiteReorder
current_bin_path="`dirname "$0"`"
cd ${current_bin_path}
sh ${current_bin_path}/../bin/submit.sh ${mainclass} wang.baozhi_${mainclass} --env pro \
        --videoType movie --offset 3000  --alg reorderALS --code movie_hollywood \
        --codeType 3 --videoTypePrefix m


