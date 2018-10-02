cp ./InnerJoin/src/main/java/InnerJoin.java ./
~/Documents/IBDM/hadoop-2.7.7/bin/hadoop com.sun.tools.javac.Main ./InnerJoin.java
jar cf ij.jar InnerJoin*.class
rm -r ./output
./hadoop-2.7.7/bin/hadoop jar ij.jar InnerJoin ./input/student_5000000.txt ./input/score_500000.txt /tmp/filter.bloom /output
