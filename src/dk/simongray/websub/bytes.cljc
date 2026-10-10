(ns ^:no-doc dk.simongray.websub.bytes
  "Bytes on both platforms: a byte array on the JVM, and a Uint8Array in
  JavaScript."
  #?(:cljs (:require [goog.crypt :as crypt]
                     [goog.crypt.base64 :as base64]
                     [goog.crypt.Hmac]
                     [goog.crypt.Sha1]
                     [goog.crypt.Sha256]
                     [goog.crypt.Sha384]
                     [goog.crypt.Sha512]))
  #?(:clj (:import [java.nio.charset StandardCharsets]
                   [java.security MessageDigest SecureRandom]
                   [java.util Base64]
                   [javax.crypto Mac]
                   [javax.crypto.spec SecretKeySpec])))

(defn encode
  "The bytes of the text `s` in UTF-8."
  [s]
  #?(:clj  (.getBytes (str s) StandardCharsets/UTF_8)
     :cljs (.encode (js/TextEncoder.) (str s))))

(defn decode
  "The text of the bytes `b` in UTF-8."
  [b]
  #?(:clj  (String. ^bytes b StandardCharsets/UTF_8)
     :cljs (.decode (js/TextDecoder.) b)))

(defn length
  "The number of bytes in `b`."
  [b]
  #?(:clj  (alength ^bytes b)
     :cljs (.-length b)))

(defn hex
  "The bytes `b` in hexadecimal, two characters in lower case for each."
  [b]
  #?(:clj  (apply str (map #(format "%02x" (bit-and % 0xff)) b))
     :cljs (crypt/byteArrayToHex b)))

(defn same?
  "Whether the bytes `a` and `b` are the same, in a time that doesn't tell
  where they differ."
  [a b]
  #?(:clj  (MessageDigest/isEqual ^bytes a ^bytes b)
     :cljs (and (= (.-length a) (.-length b))
                (let [diffs (map #(bit-xor (aget a %) (aget b %))
                                 (range (.-length a)))]
                  (zero? (reduce bit-or 0 diffs))))))

#?(:cljs
   (defn- hasher
     "A hasher of the Closure Library for `algorithm`, and the block size of
     its HMAC, as a pair."
     [algorithm]
     (case algorithm
       :sha1   [(goog.crypt.Sha1.) 64]
       :sha256 [(goog.crypt.Sha256.) 64]
       :sha384 [(goog.crypt.Sha384.) 128]
       :sha512 [(goog.crypt.Sha512.) 128])))

;; RFC 2104; FIPS 180-4
(defn hmac
  "The HMAC of the bytes `message` with the bytes `key` by `algorithm`,
  :sha1, :sha256, :sha384 or :sha512, as bytes."
  [algorithm key message]
  #?(:clj  (let [mac (str "Hmac" (.toUpperCase (name algorithm)))]
             (.doFinal (doto (Mac/getInstance mac)
                         (.init (SecretKeySpec. ^bytes key mac)))
                       ^bytes message))
     :cljs (let [[h block] (hasher algorithm)
                 mac       (goog.crypt.Hmac. h (js/Array.from key) block)]
             (js/Uint8Array.from (.getHmac mac (js/Array.from message))))))

(defn random
  "`n` random bytes from the platform's secure source."
  [n]
  #?(:clj  (let [b (byte-array n)]
             (.nextBytes (SecureRandom.) b)
             b)
     :cljs (js/crypto.getRandomValues (js/Uint8Array. n))))

;; RFC 4648, section 5
(defn base64url
  "The bytes `b` in the URL-safe alphabet of base64, without padding."
  [b]
  #?(:clj  (.encodeToString (.withoutPadding (Base64/getUrlEncoder)) b)
     :cljs (base64/encodeByteArray b base64/Alphabet.WEBSAFE_NO_PADDING)))
