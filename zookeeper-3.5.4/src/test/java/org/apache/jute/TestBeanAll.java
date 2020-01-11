package org.apache.jute;

import java.io.IOException;
import java.util.*;
import java.util.Map.Entry;


public class TestBeanAll implements Record {

    private byte byteV;
    private boolean booleanV;
    private int intV;
    private long longV;
    private float floatV;
    private double doubleV;
    private String stringV;
    private byte[] bytesV;
    private Record recodeV;
    private List<Integer> listV;
    private TreeMap<Integer, String> mapV;

    @Override
    public void deserialize(InputArchive archive, String tag) throws IOException {
        archive.startRecord(tag);

        this.byteV = archive.readByte("byteV");
        this.booleanV = archive.readBool("booleanV");
        this.intV = archive.readInt("intV");
        this.longV = archive.readLong("longV");
        this.floatV = archive.readFloat("floatV");
        this.doubleV = archive.readDouble("doubleV");
        this.stringV = archive.readString("stringV");
        this.bytesV = archive.readBuffer("bytes");

        archive.readRecord(recodeV, "recodeV");

        // list类型的反序列化
        Index vidx1 = archive.startVector("listV");
        if (vidx1 != null) {
            listV = new ArrayList<>();
            for (; !vidx1.done(); vidx1.incr()) {
                listV.add(archive.readInt("listInt"));
            }
        }
        archive.endVector("listV");

        // map类型的反序列化
        Index midx1 = archive.startMap("mapV");
        mapV = new TreeMap<>();
        for (; !midx1.done(); midx1.incr()) {
            Integer k1 = new Integer(archive.readInt("k1"));
            String v1 = archive.readString("v1");
            mapV.put(k1, v1);
        }
        archive.endMap("mapV");

        archive.endRecord(tag);
    }

    @Override
    public void serialize(OutputArchive archive, String tag) throws IOException {
        archive.startRecord(this, tag);

        archive.writeByte(byteV, "byteV");
        archive.writeBool(booleanV, "booleanV");
        archive.writeInt(intV, "intV");
        archive.writeLong(longV, "longV");
        archive.writeFloat(floatV, "floatV");
        archive.writeDouble(doubleV, "doubleV");
        archive.writeString(stringV, "stringV");
        archive.writeBuffer(bytesV, "bytes");
        archive.writeRecord(recodeV, "recodeV");

        // list类型的序列化
        archive.startVector(listV, "listV");
        if (listV != null) {
            int len1 = listV.size();
            for (int vidx1 = 0; vidx1 < len1; vidx1++) {
                archive.writeInt(listV.get(vidx1), "listInt");
            }
        }
        archive.endVector(listV, "listV");

        // map类型的序列化
        archive.startMap(mapV, "mapV");
        Set<Entry<Integer, String>> es1 = mapV.entrySet();
        for (Iterator<Entry<Integer, String>> midx1 = es1.iterator(); midx1.hasNext();) {
            Map.Entry<Integer, String> me1 = (Entry<Integer, String>) midx1.next();
            Integer k1 = (Integer) me1.getKey();
            String v1 = (String) me1.getValue();
            archive.writeInt(k1, "k1");
            archive.writeString(v1, "v1");
        }
        archive.endMap(mapV, "mapV");

        archive.endRecord(this, tag);
    }
}