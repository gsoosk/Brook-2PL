#!/bin/bash

# Target service to monitor
SERVICE_NAME="client"
# PROB_VALUES=(100)
MODE_VALUES=("slw")
DELAY_VALUES=(0)
PROB_VALUES=(0)
#PROB_VALUES=(0 10 20 30 40 50 60 70 80 90 100)
#PROB_VALUES=(0 5 10 15 20 25 30 35 40 45 50 55 60 65 70 75 80 85 90 95 100)
THREADS=(48)
#THREADS=(40 48 56 64 72 80 88 96 104 112 120 128 136)
#THREADS=(1 8 16 24 32 40 48 56 64 72 80 88 96 104 112 120 128 136)
#THREADS=(1 8 16 24 32 40)
HOT_VALUES=(0)
#HOT_VALUES=(32)
NUM_OF_ITEM=(-1)
#NUM_OF_ITEM=(20)
NUM_WAREHOUSE=(1 8 16 32 48 64)



sudo systemctl stop postgresql@12-main.service
sudo rm -rf ../rocksdb/db
sudo cp -r ../rocksdb/tpcc-checkpoint ../rocksdb/db
sudo pkill java


for WAREHOUSE in "${NUM_OF_WAREHOUSE[@]}"; do
    for MODE in "${MODE_VALUES[@]}"; do
        for HOT in "${HOT_VALUES[@]}"; do
            for THREAD in "${THREADS[@]}"; do
                for DELAY in "${DELAY_VALUES[@]}"; do
                    for ITEM in "${NUM_OF_ITEM[@]}"; do
                        for PROB in "${PROB_VALUES[@]}"; do

                            echo "Starting test for $MODE with DELAY=$DELAY PROB=$PROB and HOT_RECORDS=$HOT and ITEM_NUM=$ITEM and THREAD=$THREAD"

                            # Create and populate the .env file
                            echo "PROB=$PROB" > ".env"
                            echo "HOT=$HOT" >> ".env"
                            echo "ITEM=$ITEM" >> ".env"
                            echo "MODE=$MODE" >> ".env"
                            echo "DELAY=$DELAY" >> ".env"

                            sudo rm -rf ../rocksdb/db
                            sudo cp -r ../rocksdb/tpcc-checkpoint ../rocksdb/db


                            sleep 10

                            # sudo java -jar ../app/Server.jar 8000 localhost 5432 Listings,Items,Players > server.log 2>&1 &
                            sleep 1
                            sudo java -Xmx170G -jar ../app/Client.jar --address ../rocksdb/db --port -1 --throughput -1 --benchmark-time 30 --hot-players ../rocksdb/hotspots/hot_records_${HOT}_items --hot-listings ../rocksdb/hotspots/hot_records_${HOT}_listings --hot-selection-prob $PROB --max-threads $THREAD --max-retry 1000  --read-item-number $ITEM --max-items-threads 20 --operation-delay $DELAY --2pl-mode $MODE --benchmark-mode tpcc  --num-of-warehouses 64 --hot-warehouses $HOT  --transaction-mode storedProcedure > client.log 2>&1 &

                            # Define timeout in seconds
                            sleep 180
                            TIMEOUT=300  # 5 minutes timeout
                            ELAPSED=0
                            SLEEP_INTERVAL=5
                            # Wait for the specified service to exit
        #                    TODO: FIX
        #		    while pgrep -f "$SERVICE_NAME" > /dev/null; do
        #                        if [ $ELAPSED -ge $TIMEOUT ]; then
        #                            echo "Timeout reached. Service did not exit within the timeout period. restart!"
        #			    sudo pkill java
        #                            sleep 10
        #			    sudo java -jar Server.jar 8000 localhost 5432 Listings,Items,Players
        #                    	    sudo java -jar Client.jar --address localhost --port 8000 --throughput -1 --benchmark-time 30 --hot-players ../postgres/hot_records_32_items --hot-listings ../postgres/hot_records_32_listings --hot-selection-prob 80 --max-threads 80 --max-retry 10 --read-item-number -1 --max-items-threads 10 --operation-delay 0 --2pl-mode bamboo --benchmark-mode store --num-of-warehouses 8 --hot-warehouses 1
        #                            ELAPSED=0
        #                        fi
        #                        sleep $SLEEP_INTERVAL
        #                        ELAPSED=$((ELAPSED + SLEEP_INTERVAL))
        #                    done

                            sudo pkill java
                        done
                    done
                done
            done
        done
    done
done