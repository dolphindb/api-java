package com.xxdb;

import javax.crypto.Mac;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.PBEKeySpec;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;
import java.security.InvalidKeyException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.spec.InvalidKeySpecException;
import java.util.Base64;

public class CryptoUtils {

    private static final SecureRandom secureRandom = new SecureRandom();

    private static final int SHA256_DIGEST_LENGTH = 32; // 256 bits = 32 bytes

    public static String base64Encode(byte[] text, boolean noNewLines) {
        Base64.Encoder encoder;

        if (noNewLines) {
            // Use encoder without line breaks
            encoder = Base64.getEncoder().withoutPadding();
        } else {
            // Use standard encoder
            encoder = Base64.getEncoder();
        }

        return encoder.encodeToString(text);
    }

    public static byte[] base64Decode(String input, boolean noNewLines) {
        if (input == null)
            throw new IllegalArgumentException("Input string cannot be null");

        Base64.Decoder decoder;

        if (noNewLines) {
            // Use standard decoder (doesn't handle line breaks)
            decoder = Base64.getDecoder();
        } else {
            // Use MIME decoder (handles line breaks)
            decoder = Base64.getMimeDecoder();
        }

        return decoder.decode(input);
    }

    public static String generateNonce(int length) {
        // Generate random byte array
        byte[] buffer = new byte[length];
        secureRandom.nextBytes(buffer);

        // Base64 encode without line breaks
        return Base64.getEncoder().encodeToString(buffer);
    }

    public static byte[] pbkdf2HmacSha256(String password, byte[] salt, int iterCount) {
        try {
            // Create PBEKeySpec
            PBEKeySpec spec = new PBEKeySpec(
                    password.toCharArray(),
                    salt,
                    iterCount,
                    SHA256_DIGEST_LENGTH * 8  // Convert to bits
            );

            // Get PBKDF2WithHmacSHA256 instance
            SecretKeyFactory factory = SecretKeyFactory.getInstance("PBKDF2WithHmacSHA256");

            // Generate key
            byte[] saltedPassword = factory.generateSecret(spec).getEncoded();

            // Clear sensitive data
            spec.clearPassword();

            return saltedPassword;

        } catch (NoSuchAlgorithmException | InvalidKeySpecException e) {
            throw new RuntimeException("Failed to compute PBKDF2-HMAC-SHA256: " + e.getMessage(), e);
        }
    }

    /**
     * Calculate client key
     * @param saltedPassword Password processed with PBKDF2
     * @return Client key
     */
    public static byte[] computeClientKey(byte[] saltedPassword) {
        if (saltedPassword == null || saltedPassword.length != SHA256_DIGEST_LENGTH) {
            throw new IllegalArgumentException("Invalid salted password");
        }

        // Byte array for "Client Key"
        byte[] clientKeyData = "Client Key".getBytes();

        // Calculate HMAC
        return hmacSha256(saltedPassword, clientKeyData);
    }

    /**
     * Calculate stored key (SHA-256 hash of client key)
     * @param clientKey Client key
     * @return Stored key
     */
    public static byte[] computeStoredKey(byte[] clientKey) {
        if (clientKey == null || clientKey.length != SHA256_DIGEST_LENGTH) {
            throw new IllegalArgumentException("Invalid client key");
        }

        return sha256(clientKey);
    }

    /**
     * Calculate client signature
     * @param storedKey Stored key
     * @param authMessage Authentication message
     * @return Client signature
     */
    public static byte[] computeClientSignature(byte[] storedKey, String authMessage) {
        if (storedKey == null || storedKey.length != SHA256_DIGEST_LENGTH) {
            throw new IllegalArgumentException("Invalid stored key");
        }
        if (authMessage == null) {
            throw new IllegalArgumentException("Auth message cannot be null");
        }

        try {
            // Create HMAC-SHA256 instance
            Mac hmac = Mac.getInstance("HmacSHA256");

            // Initialize key
            SecretKeySpec secretKey = new SecretKeySpec(storedKey, "HmacSHA256");
            hmac.init(secretKey);

            // Calculate HMAC
            return hmac.doFinal(authMessage.getBytes());

        } catch (NoSuchAlgorithmException | InvalidKeyException e) {
            throw new RuntimeException("Failed to compute client signature: " + e.getMessage(), e);
        }
    }

    /**
     * Calculate proof (XOR operation between client key and client signature)
     * @param clientKey Client key
     * @param clientSignature Client signature
     * @return Proof result
     */
    public static byte[] computeProof(byte[] clientKey, byte[] clientSignature) {
        if (clientKey == null || clientSignature == null ||
                clientKey.length != clientSignature.length) {
            throw new IllegalArgumentException("Invalid input arrays");
        }

        byte[] proof = new byte[clientKey.length];
        for (int i = 0; i < clientKey.length; i++) {
            proof[i] = (byte)(clientKey[i] ^ clientSignature[i]);
        }
        return proof;
    }

    /**
     * Calculate server key
     * @param saltedPassword Password processed with PBKDF2
     * @return Server key
     */
    public static byte[] computeServerKey(byte[] saltedPassword) {
        if (saltedPassword == null || saltedPassword.length != SHA256_DIGEST_LENGTH) {
            throw new IllegalArgumentException("Invalid salted password");
        }

        try {
            // Create HMAC-SHA256 instance
            Mac hmac = Mac.getInstance("HmacSHA256");

            // Initialize key
            SecretKeySpec secretKey = new SecretKeySpec(saltedPassword, "HmacSHA256");
            hmac.init(secretKey);

            // Calculate HMAC
            return hmac.doFinal("Server Key".getBytes(StandardCharsets.UTF_8));

        } catch (NoSuchAlgorithmException | InvalidKeyException e) {
            throw new RuntimeException("Failed to compute server key: " + e.getMessage(), e);
        }
    }

    /**
     * Calculate server signature
     * @param serverKey Server key
     * @param authMessage Authentication message
     * @return Server signature
     */
    public static byte[] computeServerSignature(byte[] serverKey, String authMessage) {
        if (serverKey == null || serverKey.length != SHA256_DIGEST_LENGTH) {
            throw new IllegalArgumentException("Invalid server key");
        }
        if (authMessage == null) {
            throw new IllegalArgumentException("Auth message cannot be null");
        }

        try {
            // Create HMAC-SHA256 instance
            Mac hmac = Mac.getInstance("HmacSHA256");

            // Initialize key
            SecretKeySpec secretKey = new SecretKeySpec(serverKey, "HmacSHA256");
            hmac.init(secretKey);

            // Calculate HMAC
            return hmac.doFinal(authMessage.getBytes(StandardCharsets.UTF_8));

        } catch (NoSuchAlgorithmException | InvalidKeyException e) {
            throw new RuntimeException("Failed to compute server signature: " + e.getMessage(), e);
        }
    }



    /**
     * Calculate HMAC-SHA256
     * @param key HMAC key
     * @param data Data to calculate HMAC for
     * @return HMAC result
     */
    public static byte[] hmacSha256(byte[] key, byte[] data) {
        try {
            // Create HMAC-SHA256 instance
            Mac hmac = Mac.getInstance("HmacSHA256");

            // Initialize key
            SecretKeySpec secretKey = new SecretKeySpec(key, "HmacSHA256");
            hmac.init(secretKey);

            // Calculate HMAC
            return hmac.doFinal(data);

        } catch (NoSuchAlgorithmException | InvalidKeyException e) {
            throw new RuntimeException("Failed to compute HMAC-SHA256: " + e.getMessage(), e);
        }
    }

    /**
     * Calculate SHA-256 hash
     * @param data Data to hash
     * @return Hash result
     */
    public static byte[] sha256(byte[] data) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            return digest.digest(data);
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("SHA-256 algorithm not available", e);
        }
    }
}
