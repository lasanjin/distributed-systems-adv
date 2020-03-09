import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.UUID;

/**
 * Helper functions for ROM Message.
 * 
 * @author Sanjin & Svante
 */
public class ROMMessageUtils {

    private static final char[] HEX_ARRAY = "0123456789ABCDEF".toCharArray();

    private ROMMessageUtils() {
    }

    /**
     * Generates random, unique id.
     * 
     * @return Random id.
     */
    public static String genUID() {
        MessageDigest salt;
        String digest = "";
        try {
            salt = MessageDigest.getInstance("SHA-512");
            salt.update(UUID.randomUUID().toString().getBytes("UTF-8"));
            digest = bytesToHex(salt.digest());

        } catch (NoSuchAlgorithmException e) {
            System.out.println("NoSuchAlgorithmException: " + e.getMessage());

        } catch (UnsupportedEncodingException e) {
            System.out.println("UnsupportedEncodingException: " + e.getMessage());
        }

        return digest;
    }

    /**
     * Converts bytes to hexadecimals.
     * 
     * @param bytes Char array.
     * @return Hexadecimal string.
     */
    private static String bytesToHex(byte[] bytes) {
        char[] hexChars = new char[bytes.length * 2];

        for (int i = 0; i < bytes.length; i++) {
            int v = bytes[i] & 0xFF;
            hexChars[i * 2] = HEX_ARRAY[v >>> 4];
            hexChars[i * 2 + 1] = HEX_ARRAY[v & 0x0F];
        }

        return new String(hexChars);
    }

}
