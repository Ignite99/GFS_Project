# GFS_Project

## Done by:
Goh Nicholas  
Ho Wei Heng, Jaron  
Lawrence Chen Qing Zhao  
Nathan Chang  
Namitha Justine  

## How to run GFS code
Use one terminal to run the master nodes, this will subsequently also run the chunk server nodes

    cd master
    go run master.go

Next use another terminal to run the client

    cd client
    go run client.go

## How to run testing code
Note: The test code will only run when master node is running. If you dont want to do that then run test.sh in the test folder.

You can download the extension go test explorer to see all the tests avaliable in the test folder. You can also test each unit testcase one by one.

Make sure to restart the master server after each test to avoid potential errors.
