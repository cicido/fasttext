#!/bin/bash
source ./url_env.sh
stat_date=$1
mysubmit 50 key false ${stat_date}
#pcat
#mysubmit 20 pcat false ${stat_date} all
#ccat
#ls ${WORK_DIR}/model | sed '/all\.mode\.ftz/d' | awk -F '.' '{print $1}' > ${WORK_DIR}/log/catfile
#cat ${WORK_DIR}/log/catfile

#while read cat; do
#    echo $cat
#    mysubmit 50 ccat false ${stat_date} ${cat}
#done < ${WORK_DIR}/log/catfile
#mysubmit 20 dup false ${stat_date} 2
