#!/bin/bash

# Target service to monitor
SERVICE_NAME="client"
# PROB_VALUES=(100)
MODE_VALUES=("slw" "ww" "bamboo")
DELAY_VALUES=(0)
#PROB_VALUES=(0 50 80)
#PROB_VALUES=(100 90 80 70 60 50 40 30 20 10 0)
PROB_VALUES=(0 10 20 30 40 50 60 70 80 90 100)
#THREADS=(50 60 70 80)
THREADS=(60)
# HOT_VALUES=(16 32 64 128)
HOT_VALUES=(64)
# NUM_OF_ITEM=(-1)
NUM_OF_ITEM=(20)


sudo systemctl stop postgresql@12-main.service
sudo rm -rf ../rocksdb/db
sudo cp -r ../rocksdb/checkpoint ../rocksdb/db
sudo pkill java

for THREAD in "${THREADS[@]}"; do
    for DELAY in "${DELAY_VALUES[@]}"; do
        for ITEM in "${NUM_OF_ITEM[@]}"; do
            for HOT in "${HOT_VALUES[@]}"; do
                for MODE in "${MODE_VALUES[@]}"; do
                    for PROB in "${PROB_VALUES[@]}"; do

                        echo "Starting test for $MODE with DELAY=$DELAY PROB=$PROB and HOT_RECORDS=$HOT and ITEM_NUM=$ITEM and THREAD=$THREAD"

                        # Create and populate the .env file
                        echo "PROB=$PROB" > ".env"
                        echo "HOT=$HOT" >> ".env"
                        echo "ITEM=$ITEM" >> ".env"
                        echo "MODE=$MODE" >> ".env"
                        echo "DELAY=$DELAY" >> ".env"

                        sudo rm -rf ../rocksdb/db
                        sudo cp -r ../rocksdb/checkpoint ../rocksdb/db


                        sleep 10

                        # sudo java -jar ../app/Server.jar 8000 localhost 5432 Listings,Items,Players > server.log 2>&1 &
                        sleep 1
                        sudo java -jar ../app/Client.jar --address ../rocksdb/db --port -1 --throughput -1 --benchmark-time 30 --hot-players ../rocksdb/hotspots/hot_records_32_items --hot-listings ../rocksdb/hotspots/hot_records_32_listings --hot-selection-prob $PROB --max-threads $THREAD --max-retry 10 --read-item-number -1 --max-items-threads 10 --operation-delay $DELAY --2pl-mode $MODE --benchmark-mode store --num-of-warehouses 8 --hot-warehouses 1 --transaction-mode storedProcedure > client.log 2>&1 &

                        # Define timeout in seconds
                        sleep 60
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