#!/bin/sh

./bin/client -port 9090 -command "EXECUTE 1 ./test_results/6-4.txt ./tests/测试用例/测试用例6-4.txt 1 -1"
sleep 10
./bin/client -port 18081 -command "EXECUTE 1 ./test_results/6-5-node1.txt ./tests/测试用例/测试用例6-5.txt 1 -1"
./bin/client -port 28082 -command "EXECUTE 1 ./test_results/6-5-node2.txt ./tests/测试用例/测试用例6-5.txt 1 -1"
./bin/client -port 38083 -command "EXECUTE 1 ./test_results/6-5-node3.txt ./tests/测试用例/测试用例6-5.txt 1 -1"
./bin/client -port 48084 -command "EXECUTE 1 ./test_results/6-5-node4.txt ./tests/测试用例/测试用例6-5.txt 1 -1"
echo "node1"
cat ./test_results/6-5-node1.txt
echo "node2"
cat ./test_results/6-5-node2.txt
echo "node3"
cat ./test_results/6-5-node3.txt
echo "node4"
cat ./test_results/6-5-node4.txt