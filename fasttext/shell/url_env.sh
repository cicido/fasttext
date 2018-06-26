#!/bin/bash
SPARK_HOME=/opt/spark
pro_home=${BASH_SOURCE-$0}
pro_home="$(dirname "${pro_home}")"
WORK_DIR=$(cd $pro_home;cd ..;pwd)
echo "work dir=$WORK_DIR"

cat <<EOF >/dev/null
HDFS_DIR="hdfs:///tmp/duanxiping/fasttext"
echo "hdfs dir=${HDFS_DIR}"
rm -rf ${WORK_DIR}/{shell,lib,log}
mkdir -p ${WORK_DIR}/log
hadoop fs -get ${HDFS_DIR}/lib ${WORK_DIR}
hadoop fs -get ${HDFS_DIR}/shell ${WORK_DIR}

if [ ! -e "${WORK_DIR}/model/all.mode.ftz" ];then
    hadoop fs -get ${HDFS_DIR}/model ${WORK_DIR}
fi
EOF


# construct --jars
LIB=${WORK_DIR}"/lib"
echo "lib=${LIB}"
jarfiles="${WORK_DIR}/log/jarfiles"
ls ${LIB} > ${jarfiles}
jar=""
jarlen=$(wc -l  < ${jarfiles})
echo "jarlen:${jarlen}"
if [ "x${jarlen}" != "x0" ]; then
    jar=$(awk -v wk=${LIB} 'BEGIN{a=""}{a=a?a","wk"/"$1:wk"/"$1}END{print a}' ${jarfiles})
	if [ ! -z ${jar} ]; then jar="--jars ${jar}"; fi
fi
echo "jars=$jar"

# construct --files

myjar="${WORK_DIR}/shell/fasttext-1.0-SNAPSHOT.jar"
class="com.meizu.algo.url.UrlFastTextLess"
mysubmit(){
    echo "$@"
    submit_files=""
    num_executors=$1
    shift
    act=$1
    shift
    is_debug=$1
    shift
    stat_date=$1
    shift
    if [ $# -gt 0 ] && [ "x${act}" == "xpcat" -o "x${act}" == "xccat" ];then
        cat=$1
        model_files=${WORK_DIR}/model/${cat}.mode.ftz
	echo "model_files:${model_files}"
	submit_files="--files ${model_files}"
    fi
	
    logfile="${WORK_DIR}/log/$( echo ${class} | sed 's/.*\.//')_${stat_date}_${act}"    

    ${SPARK_HOME}/bin/spark-submit \
        --verbose \
        --master  yarn-client \
        ${submit_files} \
        --num-executors  ${num_executors} \
        --executor-cores 2 \
        --executor-memory 2G \
        --driver-memory 8G \
        --queue default \
        ${jar} \
        --class ${class} \
            ${myjar} ${act} ${is_debug} ${stat_date} "$@" > ${logfile}

    #hadoop fs -put -f ${logfile} ${HDFS_DIR}/log 2>/dev/null
}
