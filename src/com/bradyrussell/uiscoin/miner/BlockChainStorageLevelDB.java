package com.bradyrussell.uiscoin.miner;

import com.bradyrussell.uiscoin.Hash;
import com.bradyrussell.uiscoin.Util;
import com.bradyrussell.uiscoin.blockchain.BlockChainStorageBase;
import com.bradyrussell.uiscoin.transaction.Transaction;
import org.iq80.leveldb.*;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class BlockChainStorageLevelDB extends BlockChainStorageBase {
    ArrayList<Transaction> mempool;
    DB db = null;

    @Override
    public boolean open() {
        Logger logger = new Logger() {
            public void log(String message) {
                System.out.println(message);
            }
        };

        Options options = new Options();
        options.createIfMissing(true);
        options.cacheSize(100 * 1048576);
        options.logger(logger);

        DBFactory factory = null;

        try {
            factory = (DBFactory) BlockChainStorageLevelDB.class.getClassLoader().loadClass(System.getProperty("leveldb.factory", "org.iq80.leveldb.impl.Iq80DBFactory")).newInstance();
        } catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
            e.printStackTrace();
            return false;
        }

        try {
            db = factory.open(new File("blockchain_ldb"), options);
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }

        if(exists(Hash.getSHA512Bytes("blockheight"), "blockheight")) {
            byte[] bytes = get(Hash.getSHA512Bytes("blockheight"), "blockheight");
            ByteBuffer buf = ByteBuffer.wrap(bytes);
            HighestBlockHash = new byte[64];
            BlockHeight = buf.getInt();
            buf.get(HighestBlockHash);

            System.out.println("Loaded blockchain " + (BlockHeight + 1) + " blocks long. Last block: " + Util.Base64Encode(HighestBlockHash));
        }

        mempool = new ArrayList<>();
        if(exists(Hash.getSHA512Bytes("mempool"), "mempool")) {
            byte[] bytes = get(Hash.getSHA512Bytes("mempool"), "mempool");
            ByteBuffer buf = ByteBuffer.wrap(bytes);

            int NumTransactions = buf.getInt();

            for(int i = 0; i < NumTransactions; i++){
                int TransactionLength = buf.getInt();
                byte[] TransactionBytes = new byte[TransactionLength];
                buf.get(TransactionBytes);

                Transaction t = new Transaction();
                t.setBinaryData(TransactionBytes);
                if(t.Verify()) mempool.add(t);
            }
            System.out.println("Loaded mempool with "+mempool.size()+" transactions.");
        }

        return true;

    }

    private int getMempoolTransactionsSize(){
        int n = 0;
        for (Transaction transaction : mempool) {
            n+=transaction.getSize();
        }
        return n;
    }

    @Override
    public void close() {
        if(HighestBlockHash != null && BlockHeight >= 0) {
            ByteBuffer buf = ByteBuffer.allocate(68);
            buf.putInt(BlockHeight);
            buf.put(HighestBlockHash);

            put(Hash.getSHA512Bytes("blockheight"), buf.array(), "blockheight");
        }

        ByteBuffer buf = ByteBuffer.allocate(4+(4*mempool.size())+getMempoolTransactionsSize());

        buf.putInt(mempool.size());
        for (Transaction transaction : mempool) {
            buf.putInt(transaction.getSize());
            buf.put(transaction.getBinaryData());
        }
        put(Hash.getSHA512Bytes("mempool"), buf.array(), "mempool");

        try {
            db.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void addToMempool(Transaction transaction) {
        mempool.add(transaction);
    }

    @Override
    public void removeFromMempool(Transaction transaction) {
        mempool.remove(transaction);
    }

    @Override
    public List<Transaction> getMempool() {
        return mempool;
    }

    @Override
    public byte[] get(byte[] bytes, String s) {
        byte[] key = Util.ConcatArray(Hash.getSHA512Bytes(s), bytes);
        return db.get(key);
    }

    @Override
    public void put(byte[] bytes, byte[] bytes1, String s) {
        byte[] key = Util.ConcatArray(Hash.getSHA512Bytes(s), bytes);
        db.put(key,bytes1);
    }

    @Override
    public void remove(byte[] bytes, String s) {
        byte[] key = Util.ConcatArray(Hash.getSHA512Bytes(s), bytes);
        db.delete(key);
    }

    @Override
    public boolean exists(byte[] bytes, String s) {
        byte[] key = Util.ConcatArray(Hash.getSHA512Bytes(s), bytes);
        return db.get(key) != null;
    }

    @Override
    public List<byte[]> keys(String s) {
        ArrayList<byte[]> keyList = new ArrayList<>();

        DBIterator iterator = db.iterator();
        try {
            for(iterator.seekToFirst(); iterator.hasNext(); iterator.next()) {
                byte[] key = iterator.peekNext().getKey();
                byte[] dbName = new byte[64];
                byte[] realKey = new byte[key.length-64];
                System.arraycopy(key, 0, dbName, 0, 64);
                System.arraycopy(key, 64, realKey, 0, key.length-64);
                if(Arrays.equals(dbName,Hash.getSHA512Bytes(s))) keyList.add(realKey);
            }
            return keyList;
        } finally {
            // Make sure you close the iterator to avoid resource leaks.
            try {
                iterator.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
