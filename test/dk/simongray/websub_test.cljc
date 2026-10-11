(ns dk.simongray.websub-test
  "WebSub for subscribers and publishers: discovery, the requests to a hub,
  the signatures, and the callback with the three calls of a hub. A case
  of websub.rocks's subscriber tests is named by its number."
  (:require #?(:cljs [dk.simongray.websub.test-setup])
            [clojure.string :as str]
            [clojure.test :refer [deftest is testing]]
            [dk.simongray.wary-fetch :as http]
            [dk.simongray.wary-fetch.async :as async]
            [dk.simongray.wary-fetch.secret :as secret]
            [dk.simongray.wary-fetch.url :as url]
            [dk.simongray.websub :as websub]
            [dk.simongray.websub.bytes :as bytes]
            [dk.simongray.websub.test-support :refer [run]]))

(def hub "https://hub.example.org/")
(def topic "https://example.org/feed.xml")
(def callback "https://app.example.net/websub/4f1c9e2a")
(def shared-secret "a text that nobody can guess")

(defn- response
  "A response from the topic of `content-type` with the `body`, and the
  Link header `link` when it's given."
  ([content-type body]
   (response content-type body nil))
  ([content-type body link]
   {:url     topic
    :headers (cond-> {"content-type" content-type}
               link (assoc "link" link))
    :body    body}))

(defn- links
  "The Link header that names the `hub` and the `self` URL."
  [hub self]
  (str "<" hub ">; rel=\"hub\", <" self ">; rel=\"self\""))

(def atom-feed
  (str "<feed xmlns=\"http://www.w3.org/2005/Atom\">"
       "<link rel=\"hub\" href=\"" hub "\"/>"
       "<link rel=\"self\" href=\"" topic "\"/>"
       "<entry><link rel=\"self\" href=\"https://example.org/entry\"/></entry>"
       "</feed>"))

(def rss-feed
  (str "<rss version=\"2.0\" xmlns:atom10=\"http://www.w3.org/2005/Atom\">"
       "<channel xml:base=\"https://example.org/podcast/\">"
       "<atom10:link rel=\"hub\" href=\"hub\"/>"
       "<atom10:link rel=\"self\" href=\"feed.xml\"/>"
       "<link>https://example.org/</link>"
       "</channel></rss>"))

(deftest the-hubs-of-a-topic
  (testing "100: in the Link headers"
    (is (= {:hubs [hub] :topic "https://example.org/feed.json"}
           (websub/discover (response "application/json" "{}" (links hub "/feed.json"))))
        "a relative URL counts from the URL of the response"))
  (testing "101: in the head of an HTML page"
    (is (= {:hubs [hub] :topic topic}
           (websub/discover (response "text/html" (str "<!doctype html><html><head>"
                                                       "<link rel=\"hub\" href=\"" hub "\">"
                                                       "<link rel=\"self\" href=\"" topic "\">"
                                                       "</head><body></body></html>")))))
    (is (= {:hubs ["https://example.org/x/hub?a=1&b=2"] :topic "https://example.org/x/feed"}
           (websub/discover (response "text/html" (str "<head><base href=\"/x/\"><link rel=hub href=\"hub?a=1&amp;b=2\">"
                                                       "<link rel=self href=feed>"))))
        "against the base of the page, with its character references decoded")
    (is (= {:hubs [hub] :topic topic}
           (websub/discover (response "text/html" (str "<title>A blog</title><link rel=hub href=" hub
                                                       "><link rel=self href=" topic "><p>A post"))))
        "in a page without a head tag")
    (let [in-body (response "text/html" (str "<html><head><link rel=self href=" topic "></head>"
                                             "<body><link rel=hub href=" hub "></body>"))]
      (is (nil? (websub/discover in-body)) "not in the body, as WebSub 8.1 advises")
      (is (= {:hubs [hub] :topic topic} (websub/discover in-body {:body-links? true})) "unless :body-links?"))
    (is (nil? (websub/discover (response "text/html" (str "<head><link rel=self href=" topic ">"
                                                          "<!-- <link rel=hub href=" hub "> -->"
                                                          "<script>'<link rel=hub href=" hub ">'</script>"))
                               {:body-links? true}))
        "not in a comment or a script"))
  (testing "102: in an Atom feed"
    (is (= {:hubs [hub] :topic topic} (websub/discover (response "application/atom+xml" atom-feed)))
        "the feed's own links, not those of its entries"))
  (testing "103: in an RSS feed"
    (is (= {:hubs ["https://example.org/podcast/hub"] :topic "https://example.org/podcast/feed.xml"}
           (websub/discover (response "application/rss+xml" rss-feed)))
        "atom:links by any prefix, against their xml:base")
    (is (= {:hubs ["https://example.org/podcast/hub"] :topic "https://example.org/podcast/feed.xml"}
           (websub/discover (assoc (response "text/xml" rss-feed) :body (bytes/encode rss-feed))))
        "a body of bytes"))
  (testing "104: the Link headers before the document"
    (is (= {:hubs ["https://header.example.org/"] :topic topic}
           (websub/discover (response "application/atom+xml" atom-feed (links "https://header.example.org/" topic)))))
    (is (= {:hubs ["https://header.example.org/"] :topic topic}
           (websub/discover (response "application/atom+xml" atom-feed "<https://header.example.org/>; rel=\"hub\"")))
        "each relation by itself, so the self link of the feed counts when the headers name none"))
  (testing "200: a topic other than the URL that was fetched"
    (is (= "https://example.org/feed.xml?self=other"
           (:topic (websub/discover (response "text/plain" "" (links hub "/feed.xml?self=other")))))))
  (testing "links that the caller has read from the document"
    (is (= {:hubs [hub] :topic topic}
           (websub/discover (response "application/atom+xml" "not read")
                            {:links [{:rel "hub" :href hub} {:rel "self" :href topic}]})))
    (is (nil? (websub/discover (response "application/atom+xml" atom-feed) {:links []}))
        "and the body is never read then"))
  (testing "several relation types in one link, in any case"
    (is (= {:hubs [hub] :topic topic}
           (websub/discover (response "text/plain" "" (str "<" hub ">; rel=\"alternate HUB\", <" topic ">; rel=\"self canonical\""))))))
  (testing "a document without a media type"
    (is (= {:hubs [hub] :topic topic} (websub/discover {:url topic :headers {} :body atom-feed}))))
  (testing "a hub without a topic"
    (let [hub-alone (response "text/plain" "" (str "<" hub ">; rel=\"hub\""))]
      (is (= {:hubs [hub] :topic nil} (websub/discover hub-alone))
          "names a hub, but no topic to subscribe to, as WebSub 5.1 has it")
      (is (= {:hubs [hub] :topic topic} (websub/discover hub-alone {:url-as-topic? true}))
          "unless the URL of the response counts")))
  (is (nil? (websub/discover (response "application/rss+xml" "<rss><channel><oops></rss>"))) "a feed that doesn't parse")
  (is (nil? (websub/discover (response "application/atom+xml" "<feed xmlns=\"http://www.w3.org/2005/Atom\"/>"))) "no hub")
  (is (nil? (websub/discover {}))))

(deftest the-requests-of-a-subscriber
  (let [r (websub/request :subscribe hub topic callback {:lease-seconds 864000 :secret shared-secret})]
    (is (= [:post hub "application/x-www-form-urlencoded"]
           [(:method r) (:url r) (get-in r [:headers "Content-Type"])]))
    (is (= {"hub.callback"      callback
            "hub.mode"          "subscribe"
            "hub.topic"         topic
            "hub.lease_seconds" "864000"
            "hub.secret"        shared-secret}
           (url/query-params (:body (http/revealed r)))))
    (is (not (str/includes? (pr-str r) shared-secret)) "a request that is printed doesn't show the secret"))
  (is (= "https://hub.example.org/"
         (:url (websub/request :subscribe "http://hub.example.org/" topic callback {:secret shared-secret})))
      "a secret goes over https alone")
  (is (= "http://hub.example.org/"
         (:url (websub/request :subscribe "http://hub.example.org/" topic callback
                               {:secret shared-secret :secret-over-http? true})))
      "unless :secret-over-http?")
  (is (= {"hub.callback" callback "hub.mode" "unsubscribe" "hub.topic" topic "key" "k"}
         (url/query-params (:body (websub/request :unsubscribe hub topic callback {:params [["key" "k"]]}))))
      "the fields without a value are left out, and more can be given")
  (is (= "http://hub.example.org/" (:url (websub/request :subscribe "http://hub.example.org/" topic callback)))
      "the hub as it is without a secret")
  (testing "what is a mistake of the app"
    (doseq [[what f] {"a mode that is none"       #(websub/request :renew hub topic callback)
                      "a callback that is no URL" #(websub/request :subscribe hub topic "callback")
                      "a lease of no seconds"     #(websub/request :subscribe hub topic callback {:lease-seconds 0})
                      "a lease that isn't whole"  #(websub/request :subscribe hub topic callback {:lease-seconds 1.5})
                      "a secret of 200 bytes"     #(websub/request :subscribe hub topic callback
                                                                   {:secret (apply str (repeat 100 "æ"))})
                      "a secret that is empty"    #(websub/request :subscribe hub topic callback {:secret ""})}]
      (is (thrown? #?(:clj Exception :cljs js/Error) (f)) what))))

(deftest the-requests-of-a-publisher
  (is (= {"hub.mode" "publish" "hub.url" topic}
         (url/query-params (:body (websub/publish-request hub topic)))))
  (is (= {"hub.mode" "publish" "hub.topic" topic}
         (url/query-params (:body (websub/publish-request hub topic {:publish-form (assoc websub/publish-form
                                                                                         :url-field "hub.topic")}))))
      "in the form of another hub")
  (is (thrown? #?(:clj Exception :cljs js/Error) (websub/publish-request hub "feed.xml")))
  (is (= (str "<" hub ">; rel=\"hub\", <https://other.example.org/>; rel=\"hub\", <" topic ">; rel=\"self\"")
         (websub/link-header [hub "https://other.example.org/"] topic))
      "the Link header that names the hubs")
  (is (= {:hubs [hub] :topic topic}
         (websub/discover {:url "https://example.org/" :headers {}} {:links (websub/links [hub] topic)}))
      "and the links for a document, which a subscriber reads back"))

(defn- answering
  "A function that sends a request map, keeps it in the atom `sent`, and
  answers with what `f` gives for it."
  [sent f]
  (fn [request]
    (swap! sent conj request)
    (merge {:url (:url request) :headers {} :body ""} (f request))))

(deftest calling-a-hub
  (let [sent    (atom [])
        request (websub/request :subscribe hub topic callback)
        tls?    #(str/starts-with? (:url %) "https:")
        attempt (fn [request f & [opts]]
                  (async/attempt #(websub/call! request (merge {:send (answering sent f)} opts))))]
    (run
     #(async/then (websub/call! request {:send (answering sent (constantly {:status 202})) :timeout-seconds 5})
                  (fn [answer]
                    (is (= {:status 202 :url hub} answer))
                    (is (= 5 (:timeout-seconds (peek @sent))) "the request, with the limits of the app")))
     #(async/then (attempt request (constantly {:status 422 :body "The topic is not known."}))
                  (fn [{:keys [error]}]
                    (is (= {:type ::websub/failed :status 422 :message "The topic is not known."} (ex-data error))
                        "a call throws, with what the hub said")))
     #(do (reset! sent [])
          (async/then (attempt request (fn [r]
                                         (if (= hub (:url r))
                                           {:status 307 :headers {"location" ["/moved"]}}
                                           {:status 202})))
                      (fn [{:keys [value]}]
                        (is (= {:status 202 :url "https://hub.example.org/moved"} value) "203: a hub that redirects")
                        (is (= [(:body request) (:body request)] (map :body @sent)) "with the same request"))))
     #(async/then (attempt request (fn [r]
                                     (if (= hub (:url r))
                                       {:status 308 :headers {"location" ["https://new.example.org/"]}}
                                       {:status 202})))
                  (fn [{:keys [value]}]
                    (is (= "https://new.example.org/" (:url value)) "204: a hub that has moved for good")))
     #(do (reset! sent [])
          (async/then (attempt (assoc request :url "http://hub.example.org/") (constantly {:status 202}))
                      (fn [{:keys [value]}]
                        (is (= [["https://hub.example.org/" 5]] (map (juxt :url :timeout-seconds) @sent))
                            "an http hub over https first, for five seconds at most")
                        (is (= "https://hub.example.org/" (:url value))))))
     #(do (reset! sent [])
          (async/then (attempt (assoc request :url "http://hub.example.org/")
                               (fn [r]
                                 (if (tls? r)
                                   (throw (ex-info "No TLS" {}))
                                   {:status 202})))
                      (fn [{:keys [value]}]
                        (is (= ["https://hub.example.org/" "http://hub.example.org/"] (map :url @sent))
                            "and over http when that fails")
                        (is (= 202 (:status value))))))
     #(do (reset! sent [])
          (async/then (attempt (assoc request :url "http://hub.example.org/" :body (secret/hide "hub.secret=x"))
                               (fn [r]
                                 (if (tls? r)
                                   (throw (ex-info "No TLS" {}))
                                   {:status 202})))
                      (fn [{:keys [error]}]
                        (is (some? error))
                        (is (= ["https://hub.example.org/"] (map :url @sent)) "but never with a secret"))))
     #(do (reset! sent [])
          (async/then (attempt (assoc request :url "http://hub.example.org:8080/" :timeout-seconds 2)
                               (constantly {:status 202}))
                      (fn [_]
                        (is (= [["https://hub.example.org:8080/" 2]] (map (juxt :url :timeout-seconds) @sent))
                            "a hub with a port of its own too, and a shorter limit of the request's own"))))
     #(do (reset! sent [])
          (async/then (attempt (assoc request :url "http://hub.example.org/") (constantly {:status 202})
                               {:prefer-https? false})
                      (fn [_]
                        (is (= ["http://hub.example.org/"] (map :url @sent)) "as it is, unless :prefer-https?"))))
     #(do (reset! sent [])
          (async/then (attempt (assoc request :url "http://hub.example.org/") (constantly {:status 202})
                               {:https-timeout-seconds 1})
                      (fn [_]
                        (is (= [1] (map :timeout-seconds @sent)) "for :https-timeout-seconds"))))
     #(do (reset! sent [])
          (async/then (attempt (assoc request :url "http://hub.example.org/" :body (secret/hide "hub.secret=x"))
                               (fn [r]
                                 (if (tls? r)
                                   (throw (ex-info "No TLS" {}))
                                   {:status 202}))
                               {:secret-over-http? true})
                      (fn [{:keys [value]}]
                        (is (= ["https://hub.example.org/" "http://hub.example.org/"] (map :url @sent))
                            "and a secret over http too by :secret-over-http?")
                        (is (= 202 (:status value))))))
     #(async/then (attempt request (fn [_] {:status 307 :headers {"location" ["/moved"]}}) {:max-redirects 0})
                  (fn [{:keys [error]}]
                    (is (= 307 (:status (ex-data error))) "and no hub that redirects by :max-redirects 0"))))))

(deftest publishing
  (let [sent (atom [])
        send (answering sent #(if (= hub (:url %)) {:status 204} {:status 500 :body "Down"}))]
    (run
     #(async/then (websub/publish! [hub "https://other.example.org/"] topic {:send send})
                  (fn [results]
                    (is (= [{:hub hub :status 204}
                            {:hub "https://other.example.org/" :error {:reason "The hub answered 500"
                                                                       :type   ::websub/failed
                                                                       :status 500}}]
                           results)
                        "each hub that the publisher names")
                    (is (= #{{"hub.mode" "publish" "hub.url" topic}}
                           (set (map (comp url/query-params :body) @sent)))))))))

(deftest the-signature-of-a-hub
  (let [body (bytes/encode "what do ya want for nothing?")]
    (testing "the test cases of RFC 2202 and RFC 4231"
      (is (= "sha1=effcdf6ae5eb2fa2d27416d5f184df9c259a7c79" (websub/signature "sha1" "Jefe" body)))
      (is (= "sha256=5bdcc146bf60754e6a042426089575c75a003f089d2739839dec58b964ec3843"
             (websub/signature "sha256" "Jefe" body)))
      (is (= (str "sha384=af45d2e376484031617f78d2b58a6b1b9c7ef464f5a01b47"
                  "e42ec3736322445e8e2240ca5e69e2c78b3239ecfab21649")
             (websub/signature "sha384" "Jefe" body)))
      (is (= (str "sha512=164b7a7bfcf819e2e395fbe73b56e0a387bd64222e831fd610270cd7ea250554"
                  "9758bf75c05a994a6d034f65f8f0e6fdcaeab1a34d4a6b4b636e070a38bce737")
             (websub/signature "sha512" "Jefe" body))))
    (is (nil? (websub/signature "md5" "Jefe" body)) "a method that WebSub doesn't name")
    (is (= (websub/signature "sha256" "Jefe" body) (websub/signature "sha256" (secret/hide "Jefe") body))
        "a hidden secret")
    (testing "a header that is the signature"
      (is (websub/signed? "Jefe" (websub/signature "sha256" "Jefe" body) body))
      (is (websub/signed? (secret/hide "Jefe") (websub/signature "sha256" "Jefe" body) body))
      (is (websub/signed? "Jefe" (str/upper-case (websub/signature "sha1" "Jefe" body)) body)
          "in whatever case the hub writes it")
      (is (websub/signed? "Jefe" (str " " (websub/signature "sha512" "Jefe" body) " ") body)))
    (testing "a header that is not"
      (doseq [header [nil "" "sha256" "sha256=" "sha256=xyz" "md5=abc"
                      (websub/signature "sha256" "another" body)
                      (websub/signature "sha256" "Jefe" (bytes/encode "another body"))
                      (str (websub/signature "sha256" "Jefe" body) "00")]]
        (is (not (websub/signed? "Jefe" header body)) (pr-str header))))
    (is (not (websub/signed? "Jefe" (websub/signature "sha1" "Jefe" body) body #{"sha256" "sha512"}))
        "a method that the subscriber doesn't take")))

(defn- query
  "The query string of a check by the hub with `params`."
  [params]
  (url/query-string (merge {"hub.topic" topic "hub.challenge" "abc-123_DEF.456" "hub.lease_seconds" 864000}
                           params)))

(def safe
  {"Content-Type" "application/octet-stream" "X-Content-Type-Options" "nosniff"})

(deftest the-callback-of-a-subscriber
  (let [came  (atom [])
        feed  (bytes/encode "<rss version=\"2.0\"><channel><title>A show</title></channel></rss>")
        serve (websub/handler
               {:wanted-pred     (fn [facts]
                                   (swap! came conj [:wanted (dissoc facts :request)])
                                   (= topic (:topic facts)))
                :verified!       (fn [facts]
                                   (swap! came conj [:verified (dissoc facts :request)]))
                :deliver!        (fn [facts]
                                   (swap! came conj [:feed (update (dissoc facts :request) :body bytes/length)]))
                :denied!         (fn [facts]
                                   (swap! came conj [:denied (dissoc facts :request)]))
                :secret          (fn [request] (when-not (= "/open" (:uri request)) shared-secret))
                :subscribed-pred (fn [request] (not= "/gone" (:uri request)))})
        get!  (fn [params] (serve {:request-method :get :query-string (query params)}))
        post! (fn [headers body & [uri]] (serve {:request-method :post :uri uri :headers headers :body body}))]
    (testing "the hub checks that the subscriber asked"
      (is (= {:status 200 :headers safe :body "abc-123_DEF.456"} (get! {"hub.mode" "subscribe"}))
          "the challenge, in a media type that no browser takes for a page")
      (let [check {:mode :subscribe :topic topic :lease-seconds 864000}]
        (is (= [[:wanted check] [:verified check]] (take-last 2 @came))
            "and the app learns that the subscription is confirmed, with its lease"))
      (is (= 200 (:status (get! {"hub.mode" "unsubscribe"}))))
      (is (= [[:wanted {:mode :unsubscribe :topic topic}] [:verified {:mode :unsubscribe :topic topic}]]
             (take-last 2 @came))
          "without the lease, which a subscriber ignores for an unsubscription")
      (is (= {:status 404 :headers safe :body ""}
             (get! {"hub.mode" "subscribe" "hub.topic" "https://example.org/other.xml"}))
          "205: a topic that the subscriber didn't ask for")
      (is (= :wanted (first (peek @came))) "which isn't confirmed")
      (is (= "abc-123_DEF.456"
             (:body (serve {:request-method :get
                            :query-string   (str "hub.topic=mine&hub.mode=x&" (query {"hub.mode" "subscribe"}))})))
          "the hub's parameters after those of the callback itself"))
    (testing "a challenge that a subscriber doesn't give back"
      (reset! came [])
      (doseq [challenge ["<script>alert(1)</script>" "" "a b" (apply str (repeat 513 "a"))]]
        (is (= 404 (:status (get! {"hub.mode" "subscribe" "hub.challenge" challenge}))) (pr-str challenge)))
      (is (= 404 (:status (get! {"hub.mode" "renew"}))))
      (is (= 404 (:status (serve {:request-method :get}))))
      (is (= [] @came) "and the app isn't asked"))
    (testing "the hub denies a subscription"
      (is (= 200 (:status (get! {"hub.mode" "denied" "hub.reason" "The feed is private."}))))
      (is (= [:denied {:topic topic :reason "The feed is private."}] (peek @came))))
    (testing "300: the hub delivers the feed, with the signature of the secret"
      (reset! came [])
      (is (= {:status 200 :headers safe :body ""}
             (post! {"x-hub-signature" (websub/signature "sha256" shared-secret feed)
                     "content-type"    "application/rss+xml"
                     "link"            (links hub topic)}
                    feed)))
      (is (= [[:feed {:body 65 :content-type "application/rss+xml" :topic topic :hubs [hub]}]] @came))
      (is (= 200 (:status (post! {"x-hub-signature" (websub/signature "sha1" shared-secret feed)} (bytes/decode feed))))
          "a body that the server read as text")
      (is (= 2 (count @came))))
    (testing "a delivery that the app doesn't get"
      (reset! came [])
      (doseq [header ["sha256=00" (websub/signature "sha256" "a guess" feed) "md5=00"]]
        (is (= {:status 200 :headers safe :body ""} (post! {"x-hub-signature" header} feed))
            (str "301: a wrong signature gets the same answer, " (pr-str header))))
      (is (= 403 (:status (post! {} feed))) "302: a delivery without a signature is refused")
      (is (= 410 (:status (post! {} feed "/gone"))) "a subscription that is gone")
      (is (= [] @came)))
    (testing "a subscription without a secret, and the limits"
      (is (= 200 (:status (post! {} feed "/open"))))
      (is (= [[:feed {:body 65 :content-type nil :topic nil :hubs []}]] @came)
          "each delivery, and no topic without a Link header")
      (let [strict (websub/handler {:wanted-pred            (constantly true)
                                    :deliver!               identity
                                    :secret                 shared-secret
                                    :unsigned-status        401
                                    :wrong-signature-status 403})]
        (is (= [401 403] (map #(:status (strict {:request-method :post :headers % :body feed}))
                              [{} {"x-hub-signature" "sha256=00"}]))
            "answers of the subscriber's own to a delivery that isn't signed, or is signed wrong"))
      (let [small (websub/handler {:wanted-pred (constantly true) :deliver! identity :max-bytes 64 :max-challenge 5})]
        (is (= 413 (:status (small {:request-method :post :headers {} :body feed}))))
        (is (= 404 (:status (small {:request-method :get :query-string (query {"hub.mode" "subscribe"})}))))))
    (is (= {"Allow" "GET, POST"} (select-keys (:headers (serve {:request-method :put})) ["Allow"])))
    (is (= 405 (:status (serve {:request-method :put}))))
    (let [answered (atom nil)]
      (serve {:request-method :get :query-string (query {"hub.mode" "subscribe"})} #(reset! answered %) identity)
      (is (= 200 (:status @answered)) "a handler of Ring's asynchronous kind too"))
    (is (thrown? #?(:clj Exception :cljs js/Error) (websub/handler {:wanted-pred (constantly true)}))
        "a handler needs the function that gets the deliveries")))

(deftest renewals-and-tokens
  (let [at #inst "2026-10-10T12:00:00Z"]
    (is (= #inst "2026-10-19T12:00:00Z" (websub/next-renewal 864000 at)) "a tenth of a lease of ten days")
    (is (= #inst "2026-11-08T12:00:00Z" (websub/next-renewal (* 30 86400) at)) "a day before a long one")
    (is (= #inst "2026-10-10T12:54:00Z" (websub/next-renewal 3600 at)))
    (is (= #inst "2026-10-20T11:00:00Z" (websub/next-renewal 864000 at 3600)) "or an hour before"))
  (let [tokens (repeatedly 10 websub/token)]
    (is (every? #(re-matches #"[A-Za-z0-9_-]{43}" %) tokens) "in the letters of a challenge too")
    (is (= 10 (count (set tokens)))))
  (is (re-matches #"[A-Za-z0-9_-]{22}" (websub/token 16)) "of 16 bytes"))
