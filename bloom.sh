cp ./InnerJoin/src/main/java/BloomFilterConstruct.java ./
~/Documents/IBDM/hadoop-2.7.7/bin/hadoop com.sun.tools.javac.Main ./BloomFilterConstruct.java
jar cf bfc.jar BloomFilter*.class
rm -r ./filters
rm /tmp/filter.bloom
./hadoop-2.7.7/bin/hadoop jar bfc.jar BloomFilterConstruct ./input/score_500000.txt ./filters
chmod 777 /tmp/filter.bloom
