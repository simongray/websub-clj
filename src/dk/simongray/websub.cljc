(ns dk.simongray.websub
  "WebSub for subscribers and publishers: find the hubs of a topic, ask a
  hub for a subscription, answer the hub at the callback URL, and tell the
  hubs when a topic changes.

  The code follows the W3C Recommendation of 2 June 2026 at
  https://www.w3.org/TR/websub/ and cites its sections. A subscriber is a
  server with a public URL, since the hub calls its callback. The hub
  itself is in dk.simongray.websub.hub."
  (:require [clojure.string :as str]
            [dk.simongray.wary-fetch :as http]
            [dk.simongray.wary-fetch.async :as async]
            [dk.simongray.wary-fetch.headers :as headers]
            [dk.simongray.wary-fetch.secret :as secret]
            [dk.simongray.wary-fetch.url :as url]
            [dk.simongray.webcrypto :as webcrypto]
            [dk.simongray.webcrypto.bytes :as bytes]
            [dk.simongray.webcrypto.hash :as hash]
            [dk.simongray.websub.document :as document]
            [dk.simongray.websub.ring :as ring]))

;; WebSub 7.1.1
(def signature-methods
  "The hash algorithms of a signature that a subscriber takes, by their
  names in WebSub, unless its options give another set.

  Most hubs sign with sha1, which is weak. A subscriber whose callback is
  an http URL should leave it out, as WebSub 8.3 says."
  #{"sha1" "sha256" "sha384" "sha512"})

(def default-options
  "The default of each option of a subscriber or a publisher that has one,
  which follows WebSub:

  - :url-as-topic?, false: without a rel=self, a document names no topic,
    since WebSub 5.1 subscribes to that URL alone. True makes the URL of
    the response the topic.
  - :body-links?, false: an HTML page names its links in its head, as
    WebSub 8.1 advises
  - :prefer-https?, true: an http hub is tried over https first, as WebSub
    8.2 says, for :https-timeout-seconds at most, 5
  - :secret-over-http?, false: a secret goes over https alone, as WebSub
    5.1 says
  - :max-redirects, 5: how often a hub can send a request on to another
  - :signature-methods, signature-methods
  - :unsigned-status, 403: the answer to a delivery without the signature
    that its secret asks for, as WebSub 3.2.6 has it
  - :wrong-signature-status, 200: the answer to a delivery with a wrong
    signature, which WebSub 7.1.2 allows, and which tells a stranger
    nothing
  - :max-bytes, 32 MB: the largest delivery that a callback takes
  - :max-challenge, 512: the longest challenge that a callback gives
    back"
  {:url-as-topic?          false
   :body-links?            false
   :prefer-https?          true
   :https-timeout-seconds  5
   :secret-over-http?      false
   :max-redirects          5
   :signature-methods      signature-methods
   :unsigned-status        403
   :wrong-signature-status 200
   :max-bytes              (* 32 1024 1024)
   :max-challenge          512})

(def ^:no-doc algorithms
  "The hash of the HMAC of each signature method."
  {"sha1"   :sha-1
   "sha256" :sha-256
   "sha384" :sha-384
   "sha512" :sha-512})

(defn ^:no-doc later
  "The instant `seconds` after the instant `at`."
  [at seconds]
  (let [ms (+ (inst-ms at) (* 1000 seconds))]
    #?(:clj  (java.util.Date. (long ms))
       :cljs (js/Date. ms))))

;; WebSub 5.1 and 8.2: callbacks that nobody can guess, and secrets of
;; cryptographic randomness
(defn token
  "A random text of `n` bytes, 32 by default, that nobody can guess, e.g.
  for a callback URL or a secret. The bytes come from a secure source, and
  the text is in the URL-safe letters of base64."
  ([]
   (token 32))
  ([n]
   (bytes/base64url (webcrypto/random-bytes n))))

;; RFC 8288, section 3.3
(defn- relation?
  "Whether `rel` is one of the relation types of `link`, a list of words in
  any case."
  [rel link]
  (let [types (str/split (str/lower-case (str (:rel link))) #"\s+")]
    (boolean (some #{rel} types))))

(defn ^:no-doc hrefs
  "The hrefs of the `links` with the relation type `rel`, made absolute
  against `base` when there is one."
  [rel base links]
  (keep #(when (relation? rel %)
           (some->> (:href %) (document/absolute base)))
        links))

(defn- discovery
  "The hubs and the topic that the `response` names in its Link headers,
  and else in the `document-links`, a delay, by `opts`."
  [{:keys [url headers] :as response} document-links
   {:keys [url-as-topic?] :as opts}]
  (let [header-links (headers/links headers)
        find         (fn [rel]
                       (or (seq (hrefs rel url header-links))
                           (seq (hrefs rel url @document-links))))
        hubs         (find "hub")
        ;; WebSub 5.1: the topic is the rel=self URL
        topic        (or (first (find "self"))
                         (when url-as-topic? url))]
    (when hubs
      {:hubs  (vec (distinct hubs))
       :topic topic})))

;; WebSub 4 and 8.1
(defn discover
  "The hubs and the topic that the `response` names, as a map of :hubs and
  :topic, by `opts`, or nil when it names no hub. The :topic is nil when
  it names a hub but no topic, e.g. an RSS feed without a rel=self, which
  :url-as-topic? takes the URL of the response for.

  The response is a map of :url, :headers and :body, as http/send! gives
  it. For the hubs and for the topic alike, the Link headers count first,
  and the document only when they name none. A document's links are those
  in the head of an HTML page, or the atom:links of an Atom or RSS feed
  itself, not of its entries. A relative URL counts from the URL of the
  response.

  The `opts` are the :url-as-topic? and :body-links? of default-options,
  and :links, the links of the document as maps of :rel and :href, e.g.
  of a feed that you've parsed already, so that the body isn't read."
  ([response]
   (discover response {}))
  ([response opts]
   (let [opts  (merge default-options opts)
         links (delay (if (contains? opts :links)
                        (:links opts)
                        (document/links response opts)))]
     (discovery response links opts))))

(defn discover!
  "Fetch the topic at `url` with `opts`, and give its hubs and topic as
  discover does, with the :status of the response. A request that fails
  gives the :error as http/failure has it.

  The `opts` are those of discover, and those of http/fetch!, e.g. :accept
  for the representation whose rel=self is the topic, as WebSub 4.1 has
  it, and :send. In ClojureScript, it gives a promise."
  ([url]
   (discover! url {}))
  ([url opts]
   (async/then (async/attempt #(http/fetch! url opts))
               (fn [{:keys [value error]}]
                 (if error
                   {:error (http/failure error)}
                   (merge {:status (:status value)}
                          (when (<= 200 (:status value) 299)
                            (discover value opts))))))))

(defn ^:no-doc form-request
  "The request map that posts the form of `fields`, pairs of a name and a
  value, to `url`. A field without a value is left out."
  [url fields]
  ;; WebSub 5.1: a form, in UTF-8
  {:method  :post
   :url     url
   :headers {"Content-Type" "application/x-www-form-urlencoded"}
   :body    (url/query-string fields)})

(defn ^:no-doc https-url
  "The `url` with https in place of http."
  [url]
  (str/replace url #"(?i)^http:" "https:"))

(defn ^:no-doc secret?
  "Whether `s` is a secret that WebSub takes: a text of fewer than 200
  bytes."
  [s]
  ;; WebSub 5.1
  (and (string? s)
       (< 0 (bytes/length (bytes/encode s)) 200)))

(defn- request-problem
  "What's wrong with a subscription request of `mode` to `hub` for `topic`
  and `callback`, of `lease-seconds` and `secret`, or nil."
  [mode hub topic callback lease-seconds secret]
  (cond
    (not (#{:subscribe :unsubscribe} mode))
    "The mode of a request is :subscribe or :unsubscribe"

    (not (every? url/web-url? [hub topic callback]))
    "The hub, the topic and the callback are http or https URLs"

    (and (some? lease-seconds) (not (pos-int? lease-seconds)))
    "A lease is a positive number of seconds"

    (and (some? secret) (not (secret? secret)))
    "A secret is a text of fewer than 200 bytes"))

;; WebSub 5.1 and 5.1.1
(defn request
  "The request map that asks the `hub` to subscribe the `callback` URL to
  the `topic`, or to unsubscribe it, by `mode` and `opts`.

  The `mode` is :subscribe or :unsubscribe. The callback should be a URL
  that nobody can guess, one for each subscription and each renewal, e.g.
  with a token in it. The `opts` are these:

  - :lease-seconds, how long the subscription should last, which the hub
    can change
  - :secret, a text of fewer than 200 bytes, e.g. a token, with which the
    hub signs what it sends. It can be a hidden text of wary-fetch, and
    the body of the request is then hidden too.
  - :params, more fields as pairs of a name and a value, for a hub that
    asks for them
  - :secret-over-http?, as default-options has it

  A request with a secret goes to the hub over https, even when the hub is
  an http URL, unless :secret-over-http?. Such a request keeps the http
  URL under ::http-url, so that call! can say why it used https."
  ([mode hub topic callback]
   (request mode hub topic callback {}))
  ([mode hub topic callback
    {:keys [lease-seconds params secret-over-http?] :as opts}]
   {:pre [(map? opts)]}
   (let [secret   (secret/reveal (:secret opts))
         upgrade? (and secret
                       (not secret-over-http?)
                       (= "http" (url/scheme hub)))]
     (when-let [problem (request-problem mode hub topic callback
                                         lease-seconds secret)]
       (throw (ex-info problem {:mode          mode
                                :hub           hub
                                :topic         topic
                                :callback      callback
                                :lease-seconds lease-seconds})))
     ;; WebSub 5.1 and 8.2: a secret goes over https alone
     (cond-> (form-request (cond-> hub upgrade? https-url)
                           (concat [["hub.callback" callback]
                                    ["hub.mode" (name mode)]
                                    ["hub.topic" topic]
                                    ["hub.lease_seconds" lease-seconds]
                                    ["hub.secret" secret]]
                                   params))
       secret   (update :body secret/hide)
       upgrade? (assoc ::http-url hub)))))

;; WebSub 4
(defn links
  "The links that name the `hubs` of a topic and its `self` URL, the topic
  itself, as maps of :rel and :href. A publisher puts them in the link
  elements of a page or the atom:links of a feed, e.g. as Hiccup."
  [hubs self]
  (conj (mapv #(hash-map :rel "hub" :href %) hubs)
        {:rel "self" :href self}))

;; WebSub 4; RFC 8288, section 3
(defn link-header
  "The value of a Link header that names the `hubs` of a topic and its
  `self` URL, the topic itself, e.g. for a publisher's answer to a request
  for the topic."
  [hubs self]
  (->> (links hubs self)
       (map (fn [{:keys [rel href]}] (str "<" href ">; rel=\"" rel "\"")))
       (str/join ", ")))

;; WebSub 6
(def publish-form
  "The form of the request that tells a hub of a change, as most public
  hubs take it: the :fields that each request has, and the name of the
  field for the URL of the topic.

  WebSub leaves this request to the hub and the publisher, and its note
  gives this form as an example. A hub that takes another can be given it
  as the :publish-form option, e.g. with hub.topic as the :url-field."
  {:fields    {"hub.mode" "publish"}
   :url-field "hub.url"})

(defn publish-request
  "The request map that tells the `hub` that the `topic` changed, in the
  form of the :publish-form of `opts`, publish-form by default."
  ([hub topic]
   (publish-request hub topic {}))
  ([hub topic {form :publish-form :or {form publish-form} :as opts}]
   {:pre [(map? opts)]}
   (when-not (every? url/web-url? [hub topic])
     (throw (ex-info "The hub and the topic are http or https URLs"
                     {:hub hub :topic topic})))
   (form-request hub (conj (vec (:fields form)) [(:url-field form) topic]))))

(defn- message
  "The start of the text `body` of a hub's answer, or nil."
  [body]
  (when (string? body)
    (subs body 0 (min 200 (count body)))))

;; WebSub 5.1.2
(defn- answer
  "The hub's answer of `response` as a map of :status and :url, or a throw
  of ::failed for an answer that isn't a success."
  [{:keys [status url body]}]
  (if (and (integer? status) (<= 200 status 299))
    {:status status :url url}
    (throw (ex-info (str "The hub answered " status)
                    {:type    ::failed
                     :status  status
                     :message (message body)}))))

;; WebSub 5.1.2: a hub can send a request on to another hub
(defn- send-on!
  "Send the `request` with `send`, and on to the hub that a 307 or 308
  names, as many times as its :max-redirects says."
  ([send request]
   (send-on! send request (:max-redirects request)))
  ([send request n]
   (async/then (send request)
               (fn [{:keys [status headers] :as response}]
                 (let [location (when (and (#{307 308} status) (pos? n))
                                  (some->> (headers/header headers "location")
                                           (url/absolute-url (:url request))))]
                   (if location
                     (send-on! send (assoc request :url location) (dec n))
                     response))))))

(defn- secret-over-https!
  "Send the `request` with `send` to the https URL of an http hub at
  `http-url`, which it has since it holds a secret, or else throw
  ::https-failed, which says why https."
  [send request http-url]
  (async/then (async/attempt #(send-on! send request))
              (fn [{:keys [value error]}]
                (if error
                  (throw (ex-info (str "The hub " http-url " was asked over "
                                       "https, since a secret goes over https "
                                       "alone, and that failed. To send the "
                                       "secret over http, give "
                                       ":secret-over-http? true.")
                                  {:type ::https-failed
                                   :hub  http-url
                                   :url  (:url request)}
                                  error))
                  value))))

(defn- send-to-hub!
  "Send the `request` with `send` to its hub, by the `opts` of call!. An
  http hub gets it over https first, and over http when that fails, unless
  the body is a hidden text, which goes over https alone."
  [send request {:keys [prefer-https? https-timeout-seconds secret-over-http?]
                 :as   opts}]
  (let [url   (:url request)
        https (update request :url https-url)]
    (cond
      (::http-url request)
      (secret-over-https! send request (::http-url request))

      (not= "http" (url/scheme url))
      (send-on! send request)

      ;; WebSub 5.1: a secret goes over https alone
      (and (secret/hidden? (:body request))
           (not secret-over-http?))
      (secret-over-https! send https url)

      ;; WebSub 8.2: subscribers favour https for hubs
      prefer-https?
      (let [timeout (min https-timeout-seconds
                         (:timeout-seconds request https-timeout-seconds))]
        (async/then (async/attempt
                     #(send-on! send (assoc https :timeout-seconds timeout)))
                    (fn [{:keys [value]}]
                      (or value
                          (send-on! send request)))))

      :else
      (send-on! send request))))

(defn call!
  "Send the `request`, to subscribe or to publish, to its hub with `opts`,
  and give the hub's answer as a map of :status and the final :url. In
  ClojureScript, it gives a promise.

  A hub answers 202 to a subscription request that it takes, and then
  calls the callback to check it. A status that isn't a success throws
  ::failed with the :status and the :message of the hub. An http hub is
  tried over https first, and a 307 or 308 sends the request on to the
  hub it names. A request with a secret goes over https alone, and when
  that fails, the throw is ::https-failed, with the http URL of the :hub
  and the cause.

  The `opts` are the limits, :allow-pred and :user-agent of http/send!,
  :send, a function that sends a request map in place of http/send!, and
  the :prefer-https?, :https-timeout-seconds, :secret-over-http? and
  :max-redirects of default-options."
  ([request]
   (call! request {}))
  ([request opts]
   {:pre [(map? opts)]}
   (let [all     (merge {:send http/send!} default-options opts)
         request (merge (select-keys all [:max-redirects])
                        (http/with-options request opts))]
     (async/then (send-to-hub! (:send all) request all)
                 answer))))

(defn publish!
  "Tell each of the `hubs` that the `topic` changed, with `opts`, and give
  a map of each hub's :hub and its :status, or the :error as
  http/failure has it.

  The `opts` are those of call! and publish-request. In ClojureScript, it
  gives a promise."
  ([hubs topic]
   (publish! hubs topic {}))
  ([hubs topic opts]
   ;; WebSub 6: the publisher informs every hub it names
   (async/all (fn [hub]
                (async/then (async/attempt
                             #(call! (publish-request hub topic opts) opts))
                            (fn [{:keys [value error]}]
                              (if error
                                {:hub hub :error (http/failure error)}
                                {:hub hub :status (:status value)}))))
              hubs)))

;; WebSub 7.1
(defn signature
  "The signature of the bytes `body` with the text `secret`, by the hash
  algorithm `method` of signature-methods, or nil for any other method.

  It's the value of the header X-Hub-Signature: the HMAC of the body in
  hexadecimal, after the method and an equals sign, e.g. sha256=1f3a…. The
  secret can be a hidden text of wary-fetch."
  [method secret body]
  (when-let [algorithm (algorithms method)]
    (let [k (bytes/encode (secret/reveal secret))]
      (str method "=" (bytes/hex (hash/hmac algorithm k body))))))

;; WebSub 7.1.2 and 8.3
(defn signed?
  "Whether `header`, the value of X-Hub-Signature, is the signature of the
  bytes `body` with the text `secret`, by one of `methods`,
  signature-methods by default. The method is the one that the header
  names."
  ([secret header body]
   (signed? secret header body signature-methods))
  ([secret header body methods]
   (let [signature-form #"(?i)\s*([a-z0-9]{1,16})=([0-9a-f]{1,256})\s*"
         [_ method hex] (re-matches signature-form (str header))
         method         (some-> method str/lower-case)
         expected       (when (contains? methods method)
                          (signature method secret body))]
     (boolean
      (when expected
        (bytes/same? (bytes/encode expected)
                     (bytes/encode (str method "=" (str/lower-case hex)))))))))

(def ^:no-doc octets
  "The media type of the callback's answers, which no browser shows as a
  page."
  "application/octet-stream")

;; WebSub 5.3 for the characters, and 8.2 for the length
(defn- challenge?
  "Whether `s` is a challenge to give back: `limit` characters at most, of
  those that a hub may use."
  [s limit]
  (boolean (and (string? s)
                (<= 1 (count s) limit)
                (re-matches #"[+\-./0-9=A-Z_a-z]+" s))))

(defn- verification
  "The answer to the hub's GET `request` with the query `params`, by the
  `env` of the handler: to a check of intent, or to a denial."
  [{:keys [wanted-pred verified! denied! max-challenge] :as env}
   request
   params]
  (let [mode      (get params "hub.mode")
        topic     (get params "hub.topic")
        challenge (get params "hub.challenge")
        lease     (some-> (get params "hub.lease_seconds") parse-long)
        facts     {:topic topic :request request}
        ;; WebSub 5.3: the lease of an unsubscription is ignored
        check     (cond-> (assoc facts :mode (keyword mode))
                    (= "subscribe" mode) (assoc :lease-seconds lease))]
    (cond
      ;; WebSub 5.2
      (= "denied" mode)
      (do (when denied!
            (denied! (assoc facts :reason (get params "hub.reason"))))
          (ring/response 200 octets ""))

      ;; WebSub 5.3.1, and 8.2 for the media type and nosniff
      (and (#{"subscribe" "unsubscribe"} mode)
           (string? topic)
           (challenge? challenge max-challenge)
           (wanted-pred check))
      (do (when verified!
            (verified! check))
          (ring/response 200 octets challenge))

      :else
      (ring/response 404 octets ""))))

(defn- distribution
  "The answer to the hub's POST `request` with the content of a topic, by
  the `env` of the handler."
  [{:keys [secret deliver! subscribed-pred signature-methods max-bytes
           unsigned-status wrong-signature-status]
    :as   env}
   {:keys [headers] :as request}]
  (let [secret    (if (fn? secret) (secret request) secret)
        signature (get headers "x-hub-signature")
        body      (delay (ring/body-bytes (:body request) max-bytes))
        links     (headers/links headers)]
    (cond
      ;; WebSub 7: a subscription that was deleted
      (not (subscribed-pred request))
      (ring/response 410 octets "")

      (nil? @body)
      (ring/response 413 octets "")

      ;; WebSub 3.2.6: a delivery without the signature that the secret
      ;; asks for is refused
      (and secret (nil? signature))
      (ring/response unsigned-status octets "")

      ;; WebSub 7.1.2: a wrong signature is ignored
      (and secret (not (signed? secret signature @body signature-methods)))
      (ring/response wrong-signature-status octets "")

      :else
      (do (deliver! {:body         @body
                     :content-type (get headers "content-type")
                     :topic        (first (hrefs "self" nil links))
                     :hubs         (vec (hrefs "hub" nil links))
                     :request      request})
          (ring/response 200 octets "")))))

(defn handler
  "A Ring handler for the callback URL of a subscriber, by `opts`:

  - :wanted-pred, a function of the :mode, :topic, :lease-seconds and
    :request of a check, which says whether the subscriber asked
  - :verified!, a function of the same, called when the handler confirms
    a check that :wanted-pred said yes to, e.g. to keep the lease and the
    time to renew it
  - :deliver!, a function of the :body, :content-type, :topic, :hubs and
    :request of a delivery
  - :denied!, a function of the :topic, :reason and :request of a
    subscription that the hub denied or ended
  - :secret, a text or a function of the request that gives one
  - :subscribed-pred, a function of the request that says whether its
    subscription still exists, always true by default
  - the :signature-methods, :unsigned-status, :wrong-signature-status,
    :max-bytes and :max-challenge of default-options

  A hub calls the callback in three ways:

  - It checks that the subscriber asked for the :mode, :subscribe or
    :unsubscribe, of the :topic. When :wanted-pred says yes, the handler
    gives the hub's challenge back, which confirms it, and calls
    :verified!, and else it answers 404. The :lease-seconds is how long
    the hub keeps a subscription, and next-renewal tells when to renew
    it.
  - It delivers the topic, and :deliver! gets it as the bytes :body, if it
    has the signature that the :secret asks for. The handler answers 200
    when the delivery came, so :deliver! should return fast. It answers
    410 to a delivery whose subscription is gone, and 413 to one larger
    than :max-bytes.
  - It tells that it denied a subscription, or ended one, and :denied!
    gets the :topic and the :reason.

  The :topic and :hubs of a delivery are those of its Link header, which
  can differ from those of the subscription, so the callback URL tells
  the subscription, and the :request has it. In ClojureScript, a server
  gives the handler a body that it has read."
  [{:keys [wanted-pred deliver!] :as opts}]
  (when-not (and (fn? wanted-pred) (fn? deliver!))
    (throw (ex-info "A handler needs the functions :wanted-pred and :deliver!"
                    {})))
  (let [env (merge default-options
                   {:subscribed-pred (constantly true)}
                   opts)]
    (ring/handler
     (fn [{:keys [request-method query-string] :as request}]
       (case request-method
         ;; WebSub 5.1.1: the hub's parameters come after those of the
         ;; callback, so the last of each name is the hub's
         :get  (verification env request (url/query-params query-string))
         :post (distribution env request)
         ;; RFC 9110, section 15.5.6
         (assoc-in (ring/response 405 octets "")
                   [:headers "Allow"]
                   "GET, POST"))))))

;; WebSub 5.3.1: the subscriber asks again before the lease ends
(defn next-renewal
  "When to ask the hub again for a subscription whose lease of
  `lease-seconds` began at the instant `at`: `seconds-before` it ends, or
  by default a tenth of the lease before, and a day before at most."
  ([lease-seconds at]
   (next-renewal lease-seconds at (min (quot lease-seconds 10) 86400)))
  ([lease-seconds at seconds-before]
   (later at (- lease-seconds seconds-before))))

(comment
  ;; the hubs of a feed, from its headers first
  (discover {:url     "https://example.org/feed.xml"
             :headers {"link" (str "<https://hub.example.org/>; rel=\"hub\", "
                                   "</feed.xml>; rel=\"self\"")}})

  (discover {:url     "https://example.org/feed.xml"
             :headers {"content-type" "application/rss+xml"}
             :body    (str "<rss xmlns:atom=\"http://www.w3.org/2005/Atom\">"
                           "<channel><atom:link rel=\"hub\""
                           " href=\"https://hub.example.org/\"/>"
                           "</channel></rss>")})

  (def callback
    (str "https://app.example.net/websub/" (token)))

  (request :subscribe "https://hub.example.org/" "https://example.org/feed.xml"
           callback {:lease-seconds 864000 :secret (token)})

  (publish-request "https://hub.example.org/" "https://example.org/feed.xml")
  (link-header ["https://hub.example.org/"] "https://example.org/feed.xml")

  ;; a callback that wants one feed, and the calls of a hub
  (def shared-secret
    "a text that nobody can guess")

  (def serve
    (handler {:wanted-pred (fn [{:keys [topic]}]
                             (= "https://example.org/feed.xml" topic))
              :deliver!    (fn [{:keys [body]}]
                             (println (bytes/length body) "bytes came"))
              :secret      shared-secret}))

  (serve {:request-method :get
          :query-string   (str "hub.mode=subscribe"
                               "&hub.topic=https%3A%2F%2Fexample.org%2Ffeed.xml"
                               "&hub.challenge=abc123"
                               "&hub.lease_seconds=864000")})

  (def feed
    (bytes/encode "<rss><channel><title>A show</title></channel></rss>"))

  (def signed
    (signature "sha256" shared-secret feed))

  (serve {:request-method :post
          :headers        {"x-hub-signature" signed
                           "content-type"    "application/rss+xml"}
          :body           feed})

  ;; a delivery without the signature is refused
  (serve {:request-method :post :headers {} :body feed})

  (next-renewal 864000 #inst "2026-10-10T12:00:00Z")
  #_.)
