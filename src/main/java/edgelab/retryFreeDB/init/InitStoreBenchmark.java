package edgelab.retryFreeDB.init;
import org.apache.commons.io.FileUtils;
import org.rocksdb.*;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;

public class InitStoreBenchmark {
    public static final int NUM_OF_PLAYERS = 1000000;
    public static final int NUM_OF_EACH_PLAYER_ITEMS = 5;
    public static final int NUM_OF_INITIAL_LISTINGS = 100000;
    private static final double PLAYER_CASH_MIN = 10000;
    private static final double PLAYER_CASH_MAX = 50000;
    private static final double ITEM_PRICE_MIN = 1;
    private static final double ITEM_PRICE_MAX = 5;
    private static final int NAME_LENGTH = 8;
    public static final Map<String, Integer> lastIdUsed = Map.of(
            "Players", NUM_OF_PLAYERS,
            "Items", NUM_OF_PLAYERS * NUM_OF_EACH_PLAYER_ITEMS,
            "Listings", NUM_OF_INITIAL_LISTINGS
    );

    private static final ObjectMapper objectMapper = new ObjectMapper();
    private static final Random random = new Random();

    public static void main(String[] args) {
        if (args.length < 1) {
            System.out.println("Usage: java RocksDBExample <checkpoint_dir>");
            System.exit(1);
        }

        String checkpointDir = args[0];
        String hotRecordsDir = args[1];

        String dbPath;
        try {
            dbPath = Files.createTempDirectory("rocksdb-tmp").toString();
        } catch (IOException e) {
            throw new RuntimeException("Failed to create temporary directory for RocksDB", e);
        }

        System.out.println("Database path: " + dbPath);
        System.out.println("Checkpoint directory: " + checkpointDir);

        RocksDB.loadLibrary();
        try (Options options = new Options().setCreateIfMissing(true).setWriteBufferSize(16L * 1024 * 1024 * 1024)) {
            RocksDB db = RocksDB.open(options, dbPath);

            // Insert data
            insertPlayers(db);
            insertItems(db);
            insertListings(db);

            // Create checkpoint
            createCheckpoint(db, checkpointDir);


            extractHotRecords(db, hotRecordsDir);

            db.close();
        } catch (RocksDBException | IOException e) {
            e.printStackTrace();
        }
    }

    private static void insertPlayers(RocksDB db) throws RocksDBException {
        System.out.println("Generating players...");
        for (int i = 0; i < NUM_OF_PLAYERS; i++) {
            String key = "Players:" + i;
            Map<String, String> player = new HashMap<>();
            player.put("Pname", "P" + randomString(NAME_LENGTH));
            player.put("Pcash", String.valueOf( (int) (PLAYER_CASH_MIN + (PLAYER_CASH_MAX - PLAYER_CASH_MIN) * random.nextDouble())));
            db.put(key.getBytes(), objectToJson(player));
        }
        System.out.println("Players inserted.");
    }

    private static void insertItems(RocksDB db) throws RocksDBException {
        System.out.println("Generating items...");
        for (int playerId = 0; playerId < NUM_OF_PLAYERS; playerId++) {
            for (int offset = 0; offset < NUM_OF_EACH_PLAYER_ITEMS; offset++) {
                int itemId = playerId * NUM_OF_EACH_PLAYER_ITEMS + offset;
                String key = "Items:" + itemId;
                Map<String, String> item = new HashMap<>();
                item.put("IName", "I" + randomString(NAME_LENGTH));
                item.put("IOwner", String.valueOf(playerId));
                db.put(key.getBytes(), objectToJson(item));
            }
        }
        System.out.println("Items inserted.");
    }

    private static Map<String, String> itemsListings = new HashMap<>();
    private static void insertListings(RocksDB db) throws RocksDBException {
        System.out.println("Generating listings...");
        for (int i = 0; i < NUM_OF_INITIAL_LISTINGS; i++) {
            String key = "Listings:" + i;
            int randomItemId = random.nextInt((NUM_OF_PLAYERS) / 10 * NUM_OF_EACH_PLAYER_ITEMS);
            itemsListings.put(String.valueOf(randomItemId), String.valueOf(i));
            Map<String, String> listing = new HashMap<>();
            listing.put("LIId", String.valueOf(randomItemId));
            listing.put("LPrice", String.valueOf((int) (ITEM_PRICE_MIN + (ITEM_PRICE_MAX - ITEM_PRICE_MIN) * random.nextDouble())));
            db.put(key.getBytes(), objectToJson(listing));
        }
        System.out.println("Listings inserted.");
    }

    private static void createCheckpoint(RocksDB db, String checkpointDir) throws RocksDBException, IOException {
        System.out.println("Creating checkpoint...");
        File checkpointDirectory = new File(checkpointDir);
        if (checkpointDirectory.exists()) {
            FileUtils.deleteDirectory(checkpointDirectory);
        }
        Checkpoint checkpoint = Checkpoint.create(db);
        checkpoint.createCheckpoint(checkpointDir);
        System.out.println("Checkpoint created at: " + checkpointDir);
    }

    private static String randomString(int length) {
        return UUID.randomUUID().toString().replace("-", "").substring(0, length);
    }

    private static byte[] objectToJson(Object obj) {
        try {
            return objectMapper.writeValueAsBytes(obj);
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }

    private static final List<Integer> HOT_RECORDS_PLAYERS = Arrays.asList(1, 2, 4, 8, 16, 32, 64, 128, 256);



    private static void extractHotRecords(RocksDB db, String hotRecordsDir) throws RocksDBException, IOException {
        System.out.println("Extracting hot records...");
        FileUtils.deleteDirectory(Paths.get(hotRecordsDir).toFile());
        Files.createDirectories(Paths.get(hotRecordsDir));

        for (int num : HOT_RECORDS_PLAYERS) {
            List<String> randomPlayers = new ArrayList<>();
            for (int i = 0; i < num; i++) {
                randomPlayers.add("Players:" + random.nextInt(NUM_OF_PLAYERS));
            }

            List<String> itemRecords = new ArrayList<>();
            List<String> itemIds = new ArrayList<>();
            for (String playerKey : randomPlayers) {
                byte[] playerData = db.get(playerKey.getBytes());
                if (playerData != null) {
                    for (int j = 0; j < NUM_OF_EACH_PLAYER_ITEMS; j++) {
                        String itemKey = "Items:" + (Integer.parseInt(playerKey.split(":")[1]) * NUM_OF_EACH_PLAYER_ITEMS + j);
                        byte[] itemData = db.get(itemKey.getBytes());
                        if (itemData != null) {
                            String itemId = itemKey.split(":")[1];
                            itemRecords.add(itemId + "," + playerKey.split(":")[1]);
                            itemIds.add(itemId);
                        }
                    }
                }
            }
            Files.write(Paths.get(hotRecordsDir, "hot_records_" + num + "_items"), itemRecords);

            List<String> listingRecords = new ArrayList<>();
            for (String itemId : itemIds) {
                String listingKey = "Listings:" + itemsListings.get(itemId);
                byte[] listingData = db.get(listingKey.getBytes());
                if (listingData != null) {
                    Map<String, String> listingMap = objectMapper.readValue(listingData, Map.class);
                    String lId = listingKey.split(":")[1];
                    String lIId = listingMap.get("LIId");
                    String lPrice = listingMap.get("LPrice");
                    listingRecords.add(lId + "," + lIId + "," + lPrice);
                }
            }
            Files.write(Paths.get(hotRecordsDir, "hot_records_" + num + "_listings"), listingRecords);
        }
        System.out.println("Hot records extracted.");
    }
}
