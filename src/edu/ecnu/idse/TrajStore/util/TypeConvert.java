package edu.ecnu.idse.TrajStore.util;

/**
 * Created by zzg on 15-12-24.
 */
public class TypeConvert {
    /* 字符串转byte[]
        这个方法转换后的结果是会多一些 48字符进来的就是代表的是0不知道为什么，但是可以只是取出指定的字符串就行了
    */
    public static byte[] stringTo16Byte(String temp) {

        int len = temp.length();
        for (int i = 0; i < 16 - len; i++) {
            if (temp.length() == 16) {
                break;
            }
            temp = temp + "0";

        }
        return temp.getBytes();
    }

    /* byte转short */
    public final static short getShort(byte[] buf, boolean asc, int len) {
        short r = 0;
        if (asc)
            for (int i = len - 1; i >= 0; i--) {
                r <<= 8;
                r |= (buf[i] & 0x00ff);
            }
        else
            for (int i = 0; i < len; i++) {
                r <<= 8;
                r |= (buf[i] & 0x00ff);
            }

        return r;
    }

    /* B2 -> 0xB2 */
    public static int stringToByte(String in, byte[] b) throws Exception {
        if (b.length < in.length() / 2) {
            throw new Exception("byte array too small");
        }

        int j=0;
        StringBuffer buf = new StringBuffer(2);
        for (int i=0; i<in.length(); i++, j++) {
            buf.insert(0, in.charAt(i));
            buf.insert(1, in.charAt(i+1));
            int t = Integer.parseInt(buf.toString(),16);
            System.out.println("byte hex value:" + t);
            b[j] = (byte)t;
            i++;
            buf.delete(0,2);
        }

        return j;
    }

    /* byte to  int */
    public final static int getInt(byte[] buf, boolean asc, int len) {
        if (buf == null) {
            throw new IllegalArgumentException("byte array is null!");
        }
        if (len > 4) {
            throw new IllegalArgumentException("byte array size > 4 !");
        }
        int r = 0;
        if (asc)
            for (int i = len - 1; i >= 0; i--) {
                r <<= 8;
                r |= (buf[i] & 0x000000ff);
            }
        else
            for (int i = 0; i < len; i++) {
                r <<= 8;
                r |= (buf[i] & 0x000000ff);
            }
        return r;
    }

    /* int -> byte[] */
    public static byte[] intToBytes(int num) {
        byte[] b = new byte[4];
        for (int i = 0; i < 4; i++) {
            b[i] = (byte) (num >>> (24 - i * 8));
        }

        return b;
    }

    /* short to byte[] */
    public static byte[] shortToBytes(short num) {
        byte[] b = new byte[2];

        for (int i = 0; i < 2; i++) {
            b[i] = (byte) (num >>> (i * 8));
        }

        return b;
    }

    /* byte to String */
    private static char findHex(byte b) {
        int t = new Byte(b).intValue();
        t = t < 0 ? t + 16 : t;

        if ((0 <= t) &&(t <= 9)) {
            return (char)(t + '0');
        }

        return (char)(t-10+'A');
    }
    public static String byteToString(byte b) {
        byte high, low;
        byte maskHigh = (byte)0xf0;
        byte maskLow = 0x0f;

        high = (byte)((b & maskHigh) >> 4);
        low = (byte)(b & maskLow);

        StringBuffer buf = new StringBuffer();
        buf.append(findHex(high));
        buf.append(findHex(low));

        return buf.toString();
    }

    /* short -> byte */
    public final static byte[] getBytes(short s, boolean asc) {
        byte[] buf = new byte[2];
        if (asc)      for (int i = buf.length - 1; i >= 0; i--) {        buf[i] = (byte) (s & 0x00ff);
            s >>= 8;
        }
        else
            for (int i = 0; i < buf.length; i++) {
                buf[i] = (byte) (s & 0x00ff);
                s >>= 8;
            }
        return buf;
    }
    /* int -> byte[] */
    public final static byte[] getBytes(int s, boolean asc) {
        byte[] buf = new byte[4];
        if (asc)
            for (int i = buf.length - 1; i >= 0; i--) {
                buf[i] = (byte) (s & 0x000000ff);
                s >>= 8;
            }
        else
            for (int i = 0; i < buf.length; i++) {
                buf[i] = (byte) (s & 0x000000ff);
                s >>= 8;
            }
        return buf;
    }

    /* long -> byte[] */
    public final static byte[] getBytes(long s, boolean asc) {
        byte[] buf = new byte[8];
        if (asc)
            for (int i = buf.length - 1; i >= 0; i--) {
                buf[i] = (byte) (s & 0x00000000000000ff);
                s >>= 8;
            }
        else
            for (int i = 0; i < buf.length; i++) {
                buf[i] = (byte) (s & 0x00000000000000ff);
                s >>= 8;
            }
        return buf;
    }

    /* byte[]->int */
    public final static int getInt(byte[] buf, boolean asc) {
        if (buf == null) {
            throw new IllegalArgumentException("byte array is null!");
        }
        if (buf.length > 4) {
            throw new IllegalArgumentException("byte array size > 4 !");
        }
        int r = 0;
        if (asc)
            for (int i = buf.length - 1; i >= 0; i--) {
                r <<= 8;
                r |= (buf[i] & 0x000000ff);
            }
        else
            for (int i = 0; i < buf.length; i++) {
                r <<= 8;
                r |= (buf[i] & 0x000000ff);
            }
        return r;
    }
    /* byte[] -> long */
    public final static long getLong(byte[] buf, boolean asc) {
        if (buf == null) {
            throw new IllegalArgumentException("byte array is null!");
        }
        if (buf.length > 8) {
            throw new IllegalArgumentException("byte array size > 8 !");
        }
        long r = 0;
        if (asc)
            for (int i = buf.length - 1; i >= 0; i--) {
                r <<= 8;
                r |= (buf[i] & 0x00000000000000ff);
            }
        else
            for (int i = 0; i < buf.length; i++) {
                r <<= 8;
                r |= (buf[i] & 0x00000000000000ff);
            }
        return r;
    }
}