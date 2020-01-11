package org.apache.jute;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

public class TestBean implements Record {

    private int intV;

    private String stringV;

    public TestBean() {

    }

    @Override
    public void deserialize(InputArchive archive, String tag) throws IOException {
        archive.startRecord(tag);
        this.intV = archive.readInt("intV");
        this.stringV = archive.readString("stringV");
        archive.endRecord(tag);
    }

    @Override
    public void serialize(OutputArchive archive, String tag) throws IOException {
        archive.startRecord(this, tag);
        archive.writeInt(intV, "intV");
        archive.writeString(stringV, "stringV");
        archive.endRecord(this, tag);
    }

    public TestBean(int intV, String stringV) {
        this.intV = intV;
        this.stringV = stringV;
    }

    public int getIntV() {
        return intV;
    }

    public void setIntV(int intV) {
        this.intV = intV;
    }

    public String getStringV() {
        return stringV;
    }

    public void setStringV(String stringV) {
        this.stringV = stringV;
    }

    /**
     * 测试Jute序列化
     *
     * @param args
     * @throws IOException
     */
    public static void main(String[] args) throws IOException {
        String tag = "tag1";
        TestBean testBean = new TestBean(1, "testbean1");

        // 将TestBean进行序列化
        ByteArrayOutputStream byteOut = new ByteArrayOutputStream();
        BinaryOutputArchive binaryOut = BinaryOutputArchive.getArchive(byteOut);
        testBean.serialize(binaryOut, tag);
        byte array[] = byteOut.toByteArray();
        byteOut.close();


        // 将字节反序列化为Bean
        ByteArrayInputStream byteInput = new ByteArrayInputStream(array);
        BinaryInputArchive binaryInput = BinaryInputArchive.getArchive(byteInput);
        TestBean newBean = new TestBean();
        newBean.deserialize(binaryInput, tag);
        byteInput.close();

        System.out.println("intV = " + newBean.getIntV() + ",stringV = " + newBean.getStringV());

    }

}