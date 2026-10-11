(ns ^:no-doc dk.simongray.websub.ring
  "The Ring requests that a subscriber's callback and a hub take, and the
  answers they give."
  (:require [clojure.string :as str]
            [dk.simongray.wary-fetch.bytes :as bytes]
            [dk.simongray.wary-fetch.url :as url])
  #?(:clj (:import [java.io InputStream])))

(defn body-bytes
  "The bytes of the `body` of a Ring request, which is a string, bytes, a
  stream or nothing, or nil when it holds more than `max-bytes`. In
  ClojureScript a stream gives nil too, so a server there reads the body
  first."
  [body max-bytes]
  (let [b (cond
            (nil? body)
            (bytes/encode "")

            (string? body)
            (bytes/encode body)

            :else
            #?(:clj  (if (bytes? body)
                       body
                       (.readNBytes ^InputStream body (int (inc max-bytes))))
               :cljs (when (instance? js/Uint8Array body) body)))]
    (when (and b (<= (bytes/length b) max-bytes))
      b)))

;; URL Standard, section 5.1
(defn- fields
  "The fields of the form `s` in application/x-www-form-urlencoded, as pairs
  of a name and a value."
  [s]
  (let [decode #(url/percent-decode (str/replace % "+" " "))]
    (for [part  (str/split s #"&")
          :when (seq part)
          :let  [[k v] (str/split part #"=" 2)]]
      [(decode k) (decode (or v ""))])))

(defn form-fields
  "The fields of the form that the Ring `request` posts, as pairs of a name
  and a value, from its :form-params or else from a body of `max-bytes` at
  most. Nil when the body is larger."
  [{:keys [form-params body] :as request} max-bytes]
  (if (seq form-params)
    (for [[k v] form-params
          v     (if (sequential? v) v [v])]
      [(name k) v])
    (some-> (body-bytes body max-bytes) bytes/decode fields)))

(defn response
  "The Ring response of `status` with the text `body` of `media-type`, which
  a browser takes as it is rather than sniffing for another."
  [status media-type body]
  {:status  status
   :headers {"Content-Type"           media-type
             "X-Content-Type-Options" "nosniff"}
   :body    body})

(defn handler
  "A Ring handler that answers by the function `f` of a request, and that
  takes the arguments of a synchronous and of an asynchronous handler."
  [f]
  (fn
    ([request]
     (f request))
    ([request respond raise]
     (let [{:keys [answer error]} (try
                                    {:answer (f request)}
                                    (catch #?(:clj Throwable :cljs :default) e
                                      {:error e}))]
       (if error
         (raise error)
         (respond answer))))))
