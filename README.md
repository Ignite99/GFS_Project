# 50.041_GFS_Project

## Done by:
1005194 Goh Nicholas 
1005011 Ho Wei Heng, Jaron 
1005012 Lawrence Chen Qing Zhao
1005149 Nathan Chang
1005388 Namitha Justine

## How to run GFS code
Use one terminal to run the master nodes, this will subsequently also run the chunk server nodes

    cd master
    go run master.go

Next use another terminal to run the client

    cd client
    go run client.go

## How to run testing code
Note: The test code will only run when master node is running. If you dont want to do that then run test.sh in the test folder.

You can download the extension go test explorer to see all the tests avaliable in the test folder. You can also test all each unit testcase one by one.